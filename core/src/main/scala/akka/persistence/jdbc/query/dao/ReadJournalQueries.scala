/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.jdbc.query.dao

import akka.persistence.jdbc.config.{ EventJournalTableConfiguration, EventTagTableConfiguration, ReadJournalConfig }
import akka.persistence.jdbc.journal.dao.JournalTables
import slick.jdbc.JdbcProfile

class ReadJournalQueries(val profile: JdbcProfile, val readJournalConfig: ReadJournalConfig) extends JournalTables {
  override val journalTableCfg: EventJournalTableConfiguration = readJournalConfig.eventJournalTableConfiguration
  override def tagTableCfg: EventTagTableConfiguration = readJournalConfig.eventTagTableConfiguration

  import profile.api._

  def journalRowByPersistenceIds(persistenceIds: Iterable[String]) =
    for {
      query <- JournalTable.map(_.persistenceId)
      if query.inSetBind(persistenceIds)
    } yield query

  private def _allPersistenceIdsDistinct(max: ConstColumn[Long]): Query[Rep[String], String, Seq] =
    baseTableQuery().map(_.persistenceId).distinct.take(max)

  private def baseTableQuery() =
    if (readJournalConfig.includeDeleted) JournalTable
    else JournalTable.filter(_.deleted === false)

  private def baseTableWithTagsQuery() = {
    baseTableQuery().join(TagTable).on(_.ordering === _.eventId)
  }

  val allPersistenceIdsDistinct = Compiled(_allPersistenceIdsDistinct _)

  private def _messagesQuery(
      persistenceId: Rep[String],
      fromSequenceNr: Rep[Long],
      toSequenceNr: Rep[Long],
      max: ConstColumn[Long]) =
    baseTableQuery()
      .filter(_.persistenceId === persistenceId)
      .filter(_.sequenceNumber >= fromSequenceNr)
      .filter(_.sequenceNumber <= toSequenceNr)
      .sortBy(_.sequenceNumber.asc)
      .take(max)

  val messagesQuery = Compiled(_messagesQuery _)

  private def _eventsByTag(
      tag: Rep[String],
      offset: ConstColumn[Long],
      maxOffset: ConstColumn[Long],
      max: ConstColumn[Long]) = {
    baseTableWithTagsQuery()
      .filter(_._2.tag === tag)
      .sortBy(_._1.ordering.asc)
      // The following is equivalent to
      //    .filter(row => row._1.ordering > offset && row._1.ordering <= maxOffset)
      // but makes the SQL query filter over the columns on the tag table
      // which is more efficient.
      .filter(row => row._2.eventId > offset && row._2.eventId <= maxOffset)
      .take(max)
      .map(_._1)
  }

  val eventsByTag = Compiled(_eventsByTag _)

  private def _journalSequenceQuery(from: ConstColumn[Long], limit: ConstColumn[Long]) =
    JournalTable.filter(_.ordering > from).map(_.ordering).sorted.take(limit)

  val journalSequenceQuery = Compiled(_journalSequenceQuery _)

  val maxJournalSequenceQuery = Compiled {
    JournalTable.map(_.ordering).max.getOrElse(0L)
  }

}
