/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.jdbc.migrator

import akka.persistence.jdbc.config.JournalConfig
import akka.persistence.jdbc.journal.dao.JournalQueries
import slick.jdbc.JdbcBackend

import scala.concurrent.{ ExecutionContext, Future }

sealed trait JournalOrdering {
  val journalConfig: JournalConfig
  val journalQueries: JournalQueries
  val database: JdbcBackend.Database

  /**
   * the actual event journal table
   */
  val tableName: String = s"${journalConfig.eventJournalTableConfiguration.tableName}"

  /**
   * helps set the sequence ordering value in the event_journal table
   */
  def setSequenceVal(): Future[Unit]
}

case class H2(journalConfig: JournalConfig, journalQueries: JournalQueries, database: JdbcBackend.Database)(
    implicit ec: ExecutionContext)
    extends JournalOrdering {

  import slick.jdbc.H2Profile.api._

  /**
   * helps set the next ordering value in the event_journal table
   */
  override def setSequenceVal(): Future[Unit] = {
    for {
      max <- database.run(journalQueries.JournalTable.map(_.ordering).max.get.result)
      _ <- database.run(sqlu"""ALTER TABLE #$tableName ALTER COLUMN 'ordering' RESTART WITH ${max + 1}""")
    } yield ()
  }
}

case class MySQL(journalConfig: JournalConfig, journalQueries: JournalQueries, database: JdbcBackend.Database)(
    implicit ec: ExecutionContext)
    extends JournalOrdering {

  import slick.jdbc.MySQLProfile.api._

  /**
   * helps set the next ordering value in the event_journal table
   */
  override def setSequenceVal(): Future[Unit] = {
    for {
      max <- database.run(journalQueries.JournalTable.map(_.ordering).max.get.result)
      _ <- database.run(sqlu"""ALTER TABLE #$tableName AUTO_INCREMENT = ${max + 1}""")
    } yield ()
  }
}

case class Oracle(journalConfig: JournalConfig, journalQueries: JournalQueries, database: JdbcBackend.Database)(
    implicit ec: ExecutionContext)
    extends JournalOrdering {

  import slick.jdbc.OracleProfile.api._

  /**
   * helps set the next ordering value in the event_journal table
   */
  override def setSequenceVal(): Future[Unit] = {
    for {
      max <- database.run(journalQueries.JournalTable.map(_.ordering).max.get.result)
      sequenceName: String <- database.run(
        sql"""SELECT sequence_name FROM ALL_SEQUENCES WHERE sequence_name = upper($tableName || '_SEQ')"""
          .as[String]
          .head)
      _ <- database.run(sqlu"""DROP SEQUENCE #$sequenceName;""")
      _ <- database.run(sqlu"""CREATE SEQUENCE #$sequenceName START WITH ${max + 1} INCREMENT BY 1 NOMAXVALUE""")
    } yield ()
  }
}

case class Postgres(journalConfig: JournalConfig, journalQueries: JournalQueries, database: JdbcBackend.Database)(
    implicit ec: ExecutionContext)
    extends JournalOrdering {

  import slick.jdbc.PostgresProfile.api._

  /**
   * helps set the next ordering value in the event_journal table
   */
  def setSequenceVal(): Future[Unit] = {

    for {
      max <- database.run(journalQueries.JournalTable.map(_.ordering).max.get.result)
      sequenceName: String <- database.run(
        sql"""SELECT pg_get_serial_sequence($tableName, 'ordering')""".as[String].head)
      _ <- database.run(sql""" SELECT pg_catalog.setval($sequenceName, ${max + 1}, false)""".as[Long].head)
    } yield ()
  }
}

case class SqlServer(journalConfig: JournalConfig, journalQueries: JournalQueries, database: JdbcBackend.Database)(
    implicit ec: ExecutionContext)
    extends JournalOrdering {

  import slick.jdbc.SQLServerProfile.api._

  /**
   * helps set the next ordering value in the event_journal table
   */
  override def setSequenceVal(): Future[Unit] = {
    for {
      max <- database.run(journalQueries.JournalTable.map(_.ordering).max.get.result)
      _ <- database.run(sql"""DBCC CHECKIDENT (#$tableName, reseed, $max) WITH NO_INFOMSGS""".as[Long].head)
    } yield ()
  }
}
