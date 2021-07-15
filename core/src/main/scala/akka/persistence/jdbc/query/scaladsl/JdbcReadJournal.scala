/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.jdbc.query
package scaladsl

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.jdbc.config.ReadJournalConfig
import akka.persistence.jdbc.query.JournalSequenceActor.{ GetMaxOrderingId, MaxOrderingId }
import akka.persistence.jdbc.db.SlickExtension
import akka.persistence.jdbc.journal.dao.FlowControl
import akka.persistence.query.scaladsl._
import akka.persistence.query.{ EventEnvelope, Offset, Sequence }
import akka.persistence.{ Persistence, PersistentRepr }
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.{ Materializer, SystemMaterializer }
import akka.util.Timeout
import com.typesafe.config.Config
import slick.jdbc.JdbcBackend._
import slick.jdbc.JdbcProfile

import scala.collection.immutable._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }
import akka.actor.Scheduler
import akka.persistence.jdbc.query.dao.ReadJournalDao
import akka.persistence.jdbc.util.PluginVersionChecker

object JdbcReadJournal {
  final val Identifier = "jdbc-read-journal"
}

class JdbcReadJournal(config: Config, configPath: String)(implicit val system: ExtendedActorSystem)
    extends ReadJournal
    with CurrentPersistenceIdsQuery
    with PersistenceIdsQuery
    with CurrentEventsByPersistenceIdQuery
    with EventsByPersistenceIdQuery
    with CurrentEventsByTagQuery
    with EventsByTagQuery {

  PluginVersionChecker.check()

  implicit val ec: ExecutionContext = system.dispatcher
  implicit val mat: Materializer = SystemMaterializer(system).materializer

  val readJournalConfig = new ReadJournalConfig(config)

  private val writePluginId = config.getString("write-plugin")
  // If 'config' is empty, or if the plugin reference is not found, then the write plugin will be resolved from the
  // ActorSystem configuration. Otherwise, it will be resolved from the provided 'config'.
  private val eventAdapters = Persistence(system).adaptersFor(writePluginId, config)

  val readJournalDao: ReadJournalDao = {
    val slickDb = SlickExtension(system).database(config)
    val db = slickDb.database
    if (readJournalConfig.addShutdownHook && slickDb.allowShutdown) {
      system.registerOnTermination {
        db.close()
      }
    }
    val fqcn = readJournalConfig.pluginConfig.dao
    val profile: JdbcProfile = slickDb.profile
    val args = Seq(
      (classOf[Database], db),
      (classOf[JdbcProfile], profile),
      (classOf[ReadJournalConfig], readJournalConfig),
      (classOf[Serialization], SerializationExtension(system)),
      (classOf[ExecutionContext], ec),
      (classOf[Materializer], mat))
    system.dynamicAccess.createInstanceFor[ReadJournalDao](fqcn, args) match {
      case Success(dao)   => dao
      case Failure(cause) => throw cause
    }
  }

  // Started lazily to prevent the actor for querying the db if no eventsByTag queries are used
  private[query] lazy val journalSequenceActor = system.systemActorOf(
    JournalSequenceActor.props(readJournalDao, readJournalConfig.journalSequenceRetrievalConfiguration),
    s"$configPath.akka-persistence-jdbc-journal-sequence-actor")
  private val delaySource =
    Source.tick(readJournalConfig.refreshInterval, 0.seconds, 0).take(1)

  /**
   * Same type of query as `persistenceIds` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentPersistenceIds(): Source[String, NotUsed] =
    readJournalDao.allPersistenceIdsSource(Long.MaxValue)

  /**
   * `persistenceIds` is used to retrieve a stream of all `persistenceId`s as strings.
   *
   * The stream guarantees that a `persistenceId` is only emitted once and there are no duplicates.
   * Order is not defined. Multiple executions of the same stream (even bounded) may emit different
   * sequence of `persistenceId`s.
   *
   * The stream is not completed when it reaches the end of the currently known `persistenceId`s,
   * but it continues to push new `persistenceId`s when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * known `persistenceId`s is provided by `currentPersistenceIds`.
   */
  override def persistenceIds(): Source[String, NotUsed] =
    Source
      .repeat(0)
      .flatMapConcat(_ => delaySource.flatMapConcat(_ => currentPersistenceIds()))
      .statefulMapConcat[String] { () =>
        var knownIds = Set.empty[String]
        def next(id: String): Iterable[String] = {
          val xs = Set(id).diff(knownIds)
          knownIds += id
          xs
        }
        id => next(id)
      }

  private def adaptEvents(repr: PersistentRepr): Seq[PersistentRepr] = {
    val adapter = eventAdapters.get(repr.payload.getClass)
    adapter.fromJournal(repr.payload, repr.manifest).events.map(repr.withPayload)
  }

  /**
   * Same type of query as `eventsByPersistenceId` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    eventsByPersistenceIdSource(persistenceId, fromSequenceNr, toSequenceNr, None)

  /**
   * `eventsByPersistenceId` is used to retrieve a stream of events for a particular persistenceId.
   *
   * The `EventEnvelope` contains the event and provides `persistenceId` and `sequenceNr`
   * for each event. The `sequenceNr` is the sequence number for the persistent actor with the
   * `persistenceId` that persisted the event. The `persistenceId` + `sequenceNr` is an unique
   * identifier for the event.
   *
   * `fromSequenceNr` and `toSequenceNr` can be specified to limit the set of returned events.
   * The `fromSequenceNr` and `toSequenceNr` are inclusive.
   *
   * The `EventEnvelope` also provides the `offset` that corresponds to the `ordering` column in
   * the Journal table. The `ordering` is a sequential id number that uniquely identifies the
   * position of each event, also across different `persistenceId`. The `Offset` type is
   * `akka.persistence.query.Sequence` with the `ordering` as the offset value. This is the
   * same `ordering` number as is used in the offset of the `eventsByTag` query.
   *
   * The returned event stream is ordered by `sequenceNr`.
   *
   * Causality is guaranteed (`sequenceNr`s of events for a particular `persistenceId` are always ordered
   * in a sequence monotonically increasing by one). Multiple executions of the same bounded stream are
   * guaranteed to emit exactly the same stream of events.
   *
   * The stream is not completed when it reaches the end of the currently stored events,
   * but it continues to push new events when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * stored events is provided by `currentEventsByPersistenceId`.
   */
  override def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    eventsByPersistenceIdSource(
      persistenceId,
      fromSequenceNr,
      toSequenceNr,
      Some(readJournalConfig.refreshInterval -> system.scheduler))

  private def eventsByPersistenceIdSource(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      refreshInterval: Option[(FiniteDuration, Scheduler)]): Source[EventEnvelope, NotUsed] = {
    val batchSize = readJournalConfig.maxBufferSize
    readJournalDao
      .messagesWithBatch(persistenceId, fromSequenceNr, toSequenceNr, batchSize, refreshInterval)
      .mapAsync(1)(reprAndOrdNr => Future.fromTry(reprAndOrdNr))
      .mapConcat { case (repr, ordNr) =>
        adaptEvents(repr).map(_ -> ordNr)
      }
      .map { case (repr, ordNr) =>
        EventEnvelope(Sequence(ordNr), repr.persistenceId, repr.sequenceNr, repr.payload, repr.timestamp, repr.metadata)
      }
  }

  /**
   * Same type of query as `eventsByTag` but the event stream
   * is completed immediately when it reaches the end of the "result set". Events that are
   * stored after the query is completed are not included in the event stream.
   */
  override def currentEventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    currentEventsByTag(tag, offset.value)

  private def currentJournalEventsByTag(
      tag: String,
      offset: Long,
      max: Long,
      latestOrdering: MaxOrderingId): Source[EventEnvelope, NotUsed] = {
    if (latestOrdering.maxOrdering < offset) Source.empty
    else {
      readJournalDao.eventsByTag(tag, offset, latestOrdering.maxOrdering, max).mapAsync(1)(Future.fromTry).mapConcat {
        case (repr, _, ordering) =>
          adaptEvents(repr).map(r =>
            EventEnvelope(Sequence(ordering), r.persistenceId, r.sequenceNr, r.payload, r.timestamp, r.metadata))
      }
    }
  }

  /**
   * @param terminateAfterOffset If None, the stream never completes. If a Some, then the stream will complete once a
   *                             query has been executed which might return an event with this offset (or a higher offset).
   *                             The stream may include offsets higher than the value in terminateAfterOffset, since the last batch
   *                             will be returned completely.
   */
  private def eventsByTag(
      tag: String,
      offset: Long,
      terminateAfterOffset: Option[Long]): Source[EventEnvelope, NotUsed] = {
    import akka.pattern.ask
    import FlowControl._
    implicit val askTimeout: Timeout = Timeout(readJournalConfig.journalSequenceRetrievalConfiguration.askTimeout)
    val batchSize = readJournalConfig.maxBufferSize

    Source
      .unfoldAsync[(Long, FlowControl), Seq[EventEnvelope]]((offset, Continue)) { case (from, control) =>
        def retrieveNextBatch() = {
          for {
            queryUntil <- journalSequenceActor.ask(GetMaxOrderingId).mapTo[MaxOrderingId]
            xs <- currentJournalEventsByTag(tag, from, batchSize, queryUntil).runWith(Sink.seq)
          } yield {
            val hasMoreEvents = xs.size == batchSize
            val nextControl: FlowControl =
              terminateAfterOffset match {
                // we may stop if target is behind queryUntil and we don't have more events to fetch
                case Some(target) if !hasMoreEvents && target <= queryUntil.maxOrdering => Stop
                // We may also stop if we have found an event with an offset >= target
                case Some(target) if xs.exists(_.offset.value >= target) => Stop

                // otherwise, disregarding if Some or None, we must decide how to continue
                case _ =>
                  if (hasMoreEvents) Continue else ContinueDelayed
              }

            val nextStartingOffset = if (xs.isEmpty) {
              /* If no events matched the tag between `from` and `maxOrdering` then there is no need to execute the exact
               * same query again. We can continue querying from `maxOrdering`, which will save some load on the db.
               * (Note: we may never return a value smaller than `from`, otherwise we might return duplicate events) */
              math.max(from, queryUntil.maxOrdering)
            } else {
              // Continue querying from the largest offset
              xs.map(_.offset.value).max
            }
            Some(((nextStartingOffset, nextControl), xs))
          }
        }

        control match {
          case Stop     => Future.successful(None)
          case Continue => retrieveNextBatch()
          case ContinueDelayed =>
            akka.pattern.after(readJournalConfig.refreshInterval, system.scheduler)(retrieveNextBatch())
        }
      }
      .mapConcat(identity)
  }

  def currentEventsByTag(tag: String, offset: Long): Source[EventEnvelope, NotUsed] = {
    Source
      .futureSource(readJournalDao.maxJournalSequence().map { maxOrderingInDb =>
        eventsByTag(tag, offset, terminateAfterOffset = Some(maxOrderingInDb))
      })
      .mapMaterializedValue(_ => NotUsed)
  }

  /**
   * Query events that have a specific tag.
   *
   * The consumer can keep track of its current position in the event stream by storing the
   * `offset` and restart the query from a given `offset` after a crash/restart.
   * The offset is exclusive, i.e. the event corresponding to the given `offset` parameter is not
   * included in the stream.
   *
   * For Akka Persistence JDBC the `offset` corresponds to the `ordering` column in the Journal table.
   * The `ordering` is a sequential id number that uniquely identifies the position of each event within
   * the event stream. The `Offset` type is `akka.persistence.query.Sequence` with the `ordering` as the
   * offset value.
   *
   * The returned event stream is ordered by `offset`.
   *
   * In addition to the `offset` the `EventEnvelope` also provides `persistenceId` and `sequenceNr`
   * for each event. The `sequenceNr` is the sequence number for the persistent actor with the
   * `persistenceId` that persisted the event. The `persistenceId` + `sequenceNr` is an unique
   * identifier for the event.
   *
   * The stream is not completed when it reaches the end of the currently stored events,
   * but it continues to push new events when new events are persisted.
   * Corresponding query that is completed when it reaches the end of the currently
   * stored events is provided by [[CurrentEventsByTagQuery#currentEventsByTag]].
   */
  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    eventsByTag(tag, offset.value)

  def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, NotUsed] =
    eventsByTag(tag, offset, terminateAfterOffset = None)
}
