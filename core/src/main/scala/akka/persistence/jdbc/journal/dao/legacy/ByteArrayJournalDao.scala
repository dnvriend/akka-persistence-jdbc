/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.jdbc.journal.dao.legacy

import akka.persistence.jdbc.journal.dao.{ BaseDao, BaseJournalDaoWithReadMessages, H2Compat, JournalDaoWithUpdates }
import akka.persistence.jdbc.config.{ BaseDaoConfig, JournalConfig }
import akka.persistence.jdbc.serialization.FlowPersistentReprSerializer
import akka.persistence.{ AtomicWrite, PersistentRepr }
import akka.serialization.Serialization
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.{ Done, NotUsed }
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile

import scala.collection.immutable.{ Nil, Seq }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

class ByteArrayJournalDao(
    val db: Database,
    val profile: JdbcProfile,
    val journalConfig: JournalConfig,
    serialization: Serialization)(implicit val ec: ExecutionContext, val mat: Materializer)
    extends BaseByteArrayJournalDao {
  val queries = new JournalQueries(profile, journalConfig.journalTableConfiguration)
  val serializer = new ByteArrayJournalSerializer(serialization, journalConfig.pluginConfig.tagSeparator)
}

/**
 * The DefaultJournalDao contains all the knowledge to persist and load serialized journal entries
 */
trait BaseByteArrayJournalDao
    extends JournalDaoWithUpdates
    with BaseJournalDaoWithReadMessages
    with BaseDao[JournalRow]
    with H2Compat {
  val db: Database
  val profile: JdbcProfile
  val queries: JournalQueries
  val journalConfig: JournalConfig
  val baseDaoConfig: BaseDaoConfig = journalConfig.daoConfig
  val serializer: FlowPersistentReprSerializer[JournalRow]
  implicit val ec: ExecutionContext
  implicit val mat: Materializer

  import journalConfig.daoConfig.logicalDelete
  import profile.api._

  val logger = LoggerFactory.getLogger(this.getClass)

  // This logging may block since we don't control how the user will configure logback
  // We can't use a Akka logging neither because we don't have an ActorSystem in scope and
  // we should not introduce another dependency here.
  // Therefore, we make sure we only log a warning for logical deletes once
  lazy val logWarnAboutLogicalDeletionDeprecation = {
    logger.warn(
      "Logical deletion of events is deprecated and will be removed in akka.persistence.jdbc in a later version " +
      "To disable it in this current version you must set the property 'akka.persistence.jdbc.logicalDeletion.enable' to false.")
  }

  def writeJournalRows(xs: Seq[JournalRow]): Future[Unit] = { // Write atomically without auto-commit
    db.run(queries.writeJournalRows(xs).transactionally).map(_ => ())
  }

  /**
   * @see [[akka.persistence.journal.AsyncWriteJournal.asyncWriteMessages(messages)]]
   */
  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val serializedTries: Seq[Try[Seq[JournalRow]]] = serializer.serialize(messages)

    // If serialization fails for some AtomicWrites, the other AtomicWrites may still be written
    val rowsToWrite: Seq[JournalRow] = for {
      serializeTry <- serializedTries
      row <- serializeTry.getOrElse(Seq.empty)
    } yield row

    def resultWhenWriteComplete =
      if (serializedTries.forall(_.isSuccess)) Nil else serializedTries.map(_.map(_ => ()))

    queueWriteJournalRows(rowsToWrite).map(_ => resultWhenWriteComplete)
  }

  override def delete(persistenceId: String, maxSequenceNr: Long): Future[Unit] =
    if (logicalDelete) {
      // We only log a warning when user effectively deletes an event.
      // The rationale here is that this feature is not so broadly used and the default
      // is to have logical delete enabled.
      // We don't want to log warnings for users that are not using this,
      // so we make it happen only when effectively used.
      logWarnAboutLogicalDeletionDeprecation
      db.run(queries.markJournalMessagesAsDeleted(persistenceId, maxSequenceNr)).map(_ => ())
    } else {
      // We should keep journal record with highest sequence number in order to be compliant
      // with @see [[akka.persistence.journal.JournalSpec]]
      val actions: DBIOAction[Unit, NoStream, Effect.Write with Effect.Read] = for {
        _ <- queries.markJournalMessagesAsDeleted(persistenceId, maxSequenceNr)
        highestMarkedSequenceNr <- highestMarkedSequenceNr(persistenceId)
        _ <- queries.delete(persistenceId, highestMarkedSequenceNr.getOrElse(0L) - 1)
      } yield ()

      db.run(actions.transactionally)
    }

  def update(persistenceId: String, sequenceNr: Long, payload: AnyRef): Future[Done] = {
    val write = PersistentRepr(payload, sequenceNr, persistenceId)
    val serializedRow = serializer.serialize(write) match {
      case Success(t) => t
      case Failure(cause) =>
        throw new IllegalArgumentException(
          s"Failed to serialize ${write.getClass} for update of [$persistenceId] @ [$sequenceNr]",
          cause)
    }
    db.run(queries.update(persistenceId, sequenceNr, serializedRow.message).map(_ => Done))
  }

  private def highestMarkedSequenceNr(persistenceId: String) =
    queries.highestMarkedSequenceNrForPersistenceId(persistenceId).result

  override def highestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    for {
      maybeHighestSeqNo <- db.run(queries.highestSequenceNrForPersistenceId(persistenceId).result)
    } yield maybeHighestSeqNo.getOrElse(0L)

  override def messages(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long): Source[Try[(PersistentRepr, Long)], NotUsed] =
    Source
      .fromPublisher(
        db.stream(
          queries.messagesQuery((persistenceId, fromSequenceNr, toSequenceNr, correctMaxForH2Driver(max))).result))
      .via(serializer.deserializeFlow)
      .map {
        case Success((repr, _, ordering)) => Success(repr -> ordering)
        case Failure(e)                   => Failure(e)
      }

}
