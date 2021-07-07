/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.jdbc.migrator

import akka.Done
import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.jdbc.config.{ JournalConfig, ReadJournalConfig }
import akka.persistence.jdbc.db.SlickExtension
import akka.persistence.jdbc.journal.dao.{ AkkaSerialization, JournalQueries }
import akka.persistence.jdbc.journal.dao.legacy.ByteArrayJournalSerializer
import akka.persistence.jdbc.journal.dao.JournalTables.{ JournalAkkaSerializationRow, TagRow }
import akka.persistence.jdbc.query.dao.legacy.ReadJournalQueries
import akka.persistence.journal.Tagged
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.scaladsl.Source
import org.slf4j.{ Logger, LoggerFactory }
import slick.jdbc.{
  H2Profile,
  JdbcBackend,
  JdbcProfile,
  MySQLProfile,
  OracleProfile,
  PostgresProfile,
  ResultSetConcurrency,
  ResultSetType,
  SQLServerProfile
}

import scala.concurrent.{ ExecutionContextExecutor, Future }
import scala.util.{ Failure, Success }

/**
 * This will help migrate the legacy journal data onto the new journal schema with the
 * appropriate serialization
 *
 * @param system the actor system
 */
final case class JournalMigrator(profile: JdbcProfile)(implicit system: ActorSystem) {
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  import profile.api._

  val log: Logger = LoggerFactory.getLogger(getClass)

  // get the various configurations
  private val journalConfig: JournalConfig = new JournalConfig(system.settings.config.getConfig("jdbc-journal"))
  private val readJournalConfig: ReadJournalConfig = new ReadJournalConfig(
    system.settings.config.getConfig("jdbc-read-journal"))

  // the journal database
  private val journaldb: JdbcBackend.Database =
    SlickExtension(system).database(system.settings.config.getConfig("jdbc-read-journal")).database

  // get an instance of the new journal queries
  private val newJournalQueries: JournalQueries =
    new JournalQueries(profile, journalConfig.eventJournalTableConfiguration, journalConfig.eventTagTableConfiguration)

  // let us get the journal reader
  private val serialization: Serialization = SerializationExtension(system)
  private val legacyJournalQueries: ReadJournalQueries = new ReadJournalQueries(profile, readJournalConfig)
  private val serializer: ByteArrayJournalSerializer =
    new ByteArrayJournalSerializer(serialization, readJournalConfig.pluginConfig.tagSeparator)

  private val bufferSize: Int = journalConfig.daoConfig.bufferSize

  // get the journal ordering based upon the schema type used
  private val journalOrdering: JournalOrdering = profile match {
    case _: MySQLProfile     => MySQL(journalConfig, newJournalQueries, journaldb)
    case _: PostgresProfile  => Postgres(journalConfig, newJournalQueries, journaldb)
    case _: OracleProfile    => Oracle(journalConfig, newJournalQueries, journaldb)
    case _: H2Profile        => H2(journalConfig, newJournalQueries, journaldb)
    case _: SQLServerProfile => SqlServer(journalConfig, newJournalQueries, journaldb)
  }

  /**
   * write all legacy events into the new journal tables applying the proper serialization
   */
  def migrate(): Future[Done] = {
    val query =
      legacyJournalQueries.JournalTable.result
        .withStatementParameters(
          rsType = ResultSetType.ForwardOnly,
          rsConcurrency = ResultSetConcurrency.ReadOnly,
          fetchSize = bufferSize)
        .transactionally

    val eventualDone: Future[Done] = Source
      .fromPublisher(journaldb.stream(query))
      .via(serializer.deserializeFlow)
      .map {
        case Success((repr, tags, ordering)) => (repr, tags, ordering)
        case Failure(exception)              => throw exception // blow-up on failure
      }
      .map { case (repr, tags, ordering) => serialize(repr, tags, ordering) }
      // get pages of many records at once
      .grouped(bufferSize)
      .mapAsync(1)(records => {
        val stmt: DBIO[Unit] = records
          // get all the sql statements for this record as an option
          .map({ case (newRepr, newTags) =>
            log.debug(s"migrating event for PersistenceID: ${newRepr.persistenceId} with tags ${newTags.mkString(",")}")
            writeJournalRowsStatements(newRepr, newTags)
          })
          // reduce to 1 statement
          .foldLeft[DBIO[Unit]](DBIO.successful[Unit] {})((priorStmt, nextStmt) => {
            priorStmt.andThen(nextStmt)
          })

        journaldb.run(stmt)
      })
      .run()

    // run the data migration and set the next ordering value
    for {
      _ <- eventualDone
      _ <- journalOrdering.setSequenceVal() // reset the event_journal ordering to avoid collision
    } yield Done
  }

  /**
   * Unpack a PersistentRepr into a PersistentRepr and set of tags.
   * It returns a tuple containing the PersistentRepr and its set of tags
   *
   * @param pr the given PersistentRepr
   */
  private def unpackPersistentRepr(pr: PersistentRepr): (PersistentRepr, Set[String]) = {
    pr.payload match {
      case Tagged(payload, tags) => (pr.withPayload(payload), tags)
      case _                     => (pr, Set.empty[String])
    }
  }

  /**
   * serialize the PersistentRepr and construct a JournalAkkaSerializationRow and set of matching tags
   *
   * @param pr       the PersistentRepr
   * @param ordering the ordering of the PersistentRepr
   * @return the tuple of JournalAkkaSerializationRow and set of tags
   */
  private def serialize(
      repr: PersistentRepr,
      tags: Set[String],
      ordering: Long): (JournalAkkaSerializationRow, Set[String]) = {

    val serializedPayload: AkkaSerialization.AkkaSerialized =
      AkkaSerialization.serialize(serialization, repr.payload) match {
        case Failure(exception) => throw exception
        case Success(value)     => value
      }

    val serializedMetadata: Option[AkkaSerialization.AkkaSerialized] =
      repr.metadata.flatMap(m => AkkaSerialization.serialize(serialization, m).toOption)
    val row: JournalAkkaSerializationRow = JournalAkkaSerializationRow(
      ordering,
      repr.deleted,
      repr.persistenceId,
      repr.sequenceNr,
      repr.writerUuid,
      repr.timestamp,
      repr.manifest,
      serializedPayload.payload,
      serializedPayload.serId,
      serializedPayload.serManifest,
      serializedMetadata.map(_.payload),
      serializedMetadata.map(_.serId),
      serializedMetadata.map(_.serManifest))

    (row, tags)
  }

  private def writeJournalRowsStatements(
      journalSerializedRow: JournalAkkaSerializationRow,
      tags: Set[String]): DBIO[Unit] = {
    val journalInsert: DBIO[Long] = newJournalQueries.JournalTable
      .returning(newJournalQueries.JournalTable.map(_.ordering))
      .forceInsert(journalSerializedRow)

    val tagInserts =
      newJournalQueries.TagTable ++= tags.map(tag => TagRow(journalSerializedRow.ordering, tag)).toSeq

    journalInsert.flatMap(_ => tagInserts.asInstanceOf[DBIO[Unit]])
  }
}
