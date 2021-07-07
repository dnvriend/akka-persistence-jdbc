/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.jdbc.migrator

import akka.persistence.jdbc.SingleActorSystemPerTestSpec
import akka.persistence.jdbc.journal.dao.legacy
import akka.persistence.PersistentRepr
import akka.serialization.Serialization
import akka.Done
import slick.jdbc.JdbcBackend

import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }

abstract class BaseJournalMigratorSpec(config: String) extends SingleActorSystemPerTestSpec(config) {

  import profile.api._

  case class AccountOpened(accountId: String, openingBalance: Double)

  def countLegacyJournal(journaldb: JdbcBackend.Database, legacyJournalQueries: legacy.JournalQueries): Future[Int] = {
    journaldb.run(legacyJournalQueries.JournalTable.length.result)
  }

  // every specific database test will have to implement that method
  def nextOrderingValue(journaldb: JdbcBackend.Database)(implicit ec: ExecutionContext)

  private def journalRows(serialization: Serialization, numRows: Int): Seq[legacy.JournalRow] = {
    (1 to numRows).foldLeft(Seq.empty[legacy.JournalRow])((s, i) => {
      val persistenceId: String = UUID.randomUUID.toString
      s :+ legacy.JournalRow(
        ordering = i,
        deleted = false,
        persistenceId = persistenceId,
        sequenceNumber = i,
        message = serialization
          .serialize(
            PersistentRepr(
              payload = AccountOpened(accountId = s"account-$i", openingBalance = 200 * i),
              sequenceNr = i,
              persistenceId = persistenceId,
              deleted = false))
          .getOrElse(Seq.empty[Byte].toArray), // to avoid scalastyle yelling
        tags = Some(s"tag-$i"))
    })
  }

  def seedLegacyJournal(
      serialization: Serialization,
      journaldb: JdbcBackend.Database,
      legacyJournalQueries: legacy.JournalQueries,
      numRows: Int = 500000)(implicit ec: ExecutionContext): Future[Done] = {
    journaldb
      .run(
        legacyJournalQueries.JournalTable
          .returning(legacyJournalQueries.JournalTable.map(_.ordering))
          .forceInsertAll(journalRows(serialization, numRows)))
      .map(_ => Done)
  }

}
