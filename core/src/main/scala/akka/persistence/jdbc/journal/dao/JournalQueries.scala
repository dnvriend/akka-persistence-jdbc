/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.jdbc.journal.dao

import akka.persistence.jdbc.config.{ EventJournalTableConfiguration, EventTagTableConfiguration }
import akka.persistence.jdbc.journal.dao.JournalTables.{ JournalAkkaSerializationRow, TagRow }
import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext

class JournalQueries(
    val profile: JdbcProfile,
    override val journalTableCfg: EventJournalTableConfiguration,
    override val tagTableCfg: EventTagTableConfiguration)
    extends JournalTables {

  import profile.api._

  val insertAndReturn =
    JournalTable.returning(JournalTable.map(_.ordering))
  private val TagTableC = Compiled(TagTable)

  def writeJournalRows(xs: Seq[(JournalAkkaSerializationRow, Set[String])])(implicit ec: ExecutionContext) = {

    def insert(row: JournalAkkaSerializationRow, tags: Set[String]) = {
      for {
        id <- insertAndReturn += row
        _ <- TagTableC ++= tags.map(tag => TagRow(id, tag))
      } yield ()
    }

    DBIO.sequence(xs.sortBy(event => event._1.sequenceNumber).map { case (row, tags) => insert(row, tags) })
  }

  private def selectAllJournalForPersistenceIdDesc(persistenceId: Rep[String]) =
    selectAllJournalForPersistenceId(persistenceId).sortBy(_.sequenceNumber.desc)

  private def selectAllJournalForPersistenceId(persistenceId: Rep[String]) =
    JournalTable.filter(_.persistenceId === persistenceId).sortBy(_.sequenceNumber.desc)

  def delete(persistenceId: String, toSequenceNr: Long) = {
    JournalTable.filter(_.persistenceId === persistenceId).filter(_.sequenceNumber <= toSequenceNr).delete
  }

  def markJournalMessagesAsDeleted(persistenceId: String, maxSequenceNr: Long) =
    JournalTable
      .filter(_.persistenceId === persistenceId)
      .filter(_.sequenceNumber <= maxSequenceNr)
      .filter(_.deleted === false)
      .map(_.deleted)
      .update(true)

  private def _highestSequenceNrForPersistenceId(persistenceId: Rep[String]): Rep[Option[Long]] =
    selectAllJournalForPersistenceId(persistenceId).map(_.sequenceNumber).max

  private def _highestMarkedSequenceNrForPersistenceId(persistenceId: Rep[String]): Rep[Option[Long]] =
    selectAllJournalForPersistenceId(persistenceId).filter(_.deleted === true).map(_.sequenceNumber).max

  val highestSequenceNrForPersistenceId = Compiled(_highestSequenceNrForPersistenceId _)

  val highestMarkedSequenceNrForPersistenceId = Compiled(_highestMarkedSequenceNrForPersistenceId _)

  private def _selectByPersistenceIdAndMaxSequenceNumber(persistenceId: Rep[String], maxSequenceNr: Rep[Long]) =
    selectAllJournalForPersistenceIdDesc(persistenceId).filter(_.sequenceNumber <= maxSequenceNr)

  val selectByPersistenceIdAndMaxSequenceNumber = Compiled(_selectByPersistenceIdAndMaxSequenceNumber _)

  private def _allPersistenceIdsDistinct: Query[Rep[String], String, Seq] =
    JournalTable.map(_.persistenceId).distinct

  val allPersistenceIdsDistinct = Compiled(_allPersistenceIdsDistinct)

  def journalRowByPersistenceIds(persistenceIds: Iterable[String]): Query[Rep[String], String, Seq] =
    for {
      query <- JournalTable.map(_.persistenceId)
      if query.inSetBind(persistenceIds)
    } yield query

  private def _messagesQuery(
      persistenceId: Rep[String],
      fromSequenceNr: Rep[Long],
      toSequenceNr: Rep[Long],
      max: ConstColumn[Long]) =
    JournalTable
      .filter(_.persistenceId === persistenceId)
      .filter(_.deleted === false)
      .filter(_.sequenceNumber >= fromSequenceNr)
      .filter(_.sequenceNumber <= toSequenceNr)
      .sortBy(_.sequenceNumber.asc)
      .take(max)

  val messagesQuery = Compiled(_messagesQuery _)

}
