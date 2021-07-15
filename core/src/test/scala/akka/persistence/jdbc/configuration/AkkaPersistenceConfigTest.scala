/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.jdbc.configuration

import akka.persistence.jdbc.config._
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AkkaPersistenceConfigTest extends AnyFlatSpec with Matchers {
  private val referenceConfig: Config = ConfigFactory.load("reference")

  val config: Config = ConfigFactory
    .parseString("""
      |akka-persistence-jdbc.slick.db {
      |  host = <not used>
      |  port = <not used>
      |  name = <not used>
      |}
      |
      |jdbc-journal {
      |  class = "akka.persistence.jdbc.journal.JdbcAsyncWriteJournal"
      |
      |  tables {
      |    journal {
      |      tableName = "journal"
      |      schemaName = ""
      |      columnNames {
      |        ordering = "ordering"
      |        persistenceId = "persistence_id"
      |        sequenceNumber = "sequence_number"
      |        deleted = "deleted"
      |        tags = "tags"
      |        message = "message"
      |      }
      |    }
      |  }
      |
      |  tagSeparator = ","
      |
      |  dao = "akka.persistence.jdbc.dao.bytea.journal.ByteArrayJournalDao"
      |
      |  logicalDelete = true
      |
      |  slick {
      |    profile = "slick.jdbc.PostgresProfile$"
      |    db {
      |      host = "localhost"
      |      host = ${?POSTGRES_HOST}
      |      port = "5432"
      |      port = ${?POSTGRES_PORT}
      |      name = "docker"
      |
      |      url = "jdbc:postgresql://"${akka.persistence.jdbc.slick.db.host}":"${akka.persistence.jdbc.slick.db.port}"/"${akka.persistence.jdbc.slick.db.name}
      |      user = "docker"
      |      password = "docker"
      |      driver = "org.postgresql.Driver$"
      |
      |      // hikariCP settings; see: https://github.com/brettwooldridge/HikariCP
      |
      |      // read: https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing
      |      // slick will use an async executor with a fixed size queue of 10.000 objects
      |      // The async executor is a connection pool for asynchronous execution of blocking I/O actions.
      |      // This is used for the asynchronous query execution API on top of blocking back-ends like JDBC.
      |      queueSize = 10000 // number of objects that can be queued by the async exector
      |
      |      connectionTimeout = 30000 // This property controls the maximum number of milliseconds that a client (that's you) will wait for a connection from the pool. If this time is exceeded without a connection becoming available, a SQLException will be thrown. 1000ms is the minimum value. Default: 30000 (30 seconds)
      |      validationTimeout = 5000 // This property controls the maximum amount of time that a connection will be tested for aliveness. This value must be less than the connectionTimeout. The lowest accepted validation timeout is 1000ms (1 second). Default: 5000
      |      idleTimeout = 600000 // 10 minutes: This property controls the maximum amount of time that a connection is allowed to sit idle in the pool. Whether a connection is retired as idle or not is subject to a maximum variation of +30 seconds, and average variation of +15 seconds. A connection will never be retired as idle before this timeout. A value of 0 means that idle connections are never removed from the pool. Default: 600000 (10 minutes)
      |      maxLifetime = 1800000 // 30 minutes: This property controls the maximum lifetime of a connection in the pool. When a connection reaches this timeout it will be retired from the pool, subject to a maximum variation of +30 seconds. An in-use connection will never be retired, only when it is closed will it then be removed. We strongly recommend setting this value, and it should be at least 30 seconds less than any database-level connection timeout. A value of 0 indicates no maximum lifetime (infinite lifetime), subject of course to the idleTimeout setting. Default: 1800000 (30 minutes)
      |      leakDetectionThreshold = 0 // This property controls the amount of time that a connection can be out of the pool before a message is logged indicating a possible connection leak. A value of 0 means leak detection is disabled. Lowest acceptable value for enabling leak detection is 2000 (2 secs). Default: 0
      |
      |      initializationFailFast = true // This property controls whether the pool will "fail fast" if the pool cannot be seeded with initial connections successfully. If you want your application to start even when the database is down/unavailable, set this property to false. Default: true
      |
      |      keepAliveConnection = on // ensures that the database does not get dropped while we are using it
      |
      |      numThreads = 4 // number of cores
      |      maxConnections = 4  // same as numThreads
      |      minConnections = 4  // same as numThreads
      |    }
      |  }
      |}
      |
      |# the akka-persistence-snapshot-store in use
      |jdbc-snapshot-store {
      |  class = "akka.persistence.jdbc.snapshot.JdbcSnapshotStore"
      |
      |  tables {
      |    snapshot {
      |      tableName = "snapshot"
      |      schemaName = ""
      |      columnNames {
      |        persistenceId = "persistence_id"
      |        sequenceNumber = "sequence_number"
      |        created = "created"
      |        snapshot = "snapshot"
      |      }
      |    }
      |  }
      |
      |  dao = "akka.persistence.jdbc.dao.bytea.snapshot.ByteArraySnapshotDao"
      |
      |  slick {
      |    profile = "slick.jdbc.MySQLProfile$"
      |    db {
      |      host = "localhost"
      |      host = ${?POSTGRES_HOST}
      |      port = "5432"
      |      port = ${?POSTGRES_PORT}
      |      name = "docker"
      |
      |      url = "jdbc:postgresql://"${akka.persistence.jdbc.slick.db.host}":"${akka.persistence.jdbc.slick.db.port}"/"${akka.persistence.jdbc.slick.db.name}
      |      user = "docker"
      |      password = "docker"
      |      driver = "org.postgresql.Driver"
      |
      |      // hikariCP settings; see: https://github.com/brettwooldridge/HikariCP
      |
      |      // read: https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing
      |      // slick will use an async executor with a fixed size queue of 10.000 objects
      |      // The async executor is a connection pool for asynchronous execution of blocking I/O actions.
      |      // This is used for the asynchronous query execution API on top of blocking back-ends like JDBC.
      |      queueSize = 10000 // number of objects that can be queued by the async exector
      |
      |      connectionTimeout = 30000 // This property controls the maximum number of milliseconds that a client (that's you) will wait for a connection from the pool. If this time is exceeded without a connection becoming available, a SQLException will be thrown. 1000ms is the minimum value. Default: 30000 (30 seconds)
      |      validationTimeout = 5000 // This property controls the maximum amount of time that a connection will be tested for aliveness. This value must be less than the connectionTimeout. The lowest accepted validation timeout is 1000ms (1 second). Default: 5000
      |      idleTimeout = 600000 // 10 minutes: This property controls the maximum amount of time that a connection is allowed to sit idle in the pool. Whether a connection is retired as idle or not is subject to a maximum variation of +30 seconds, and average variation of +15 seconds. A connection will never be retired as idle before this timeout. A value of 0 means that idle connections are never removed from the pool. Default: 600000 (10 minutes)
      |      maxLifetime = 1800000 // 30 minutes: This property controls the maximum lifetime of a connection in the pool. When a connection reaches this timeout it will be retired from the pool, subject to a maximum variation of +30 seconds. An in-use connection will never be retired, only when it is closed will it then be removed. We strongly recommend setting this value, and it should be at least 30 seconds less than any database-level connection timeout. A value of 0 indicates no maximum lifetime (infinite lifetime), subject of course to the idleTimeout setting. Default: 1800000 (30 minutes)
      |      leakDetectionThreshold = 0 // This property controls the amount of time that a connection can be out of the pool before a message is logged indicating a possible connection leak. A value of 0 means leak detection is disabled. Lowest acceptable value for enabling leak detection is 2000 (2 secs). Default: 0
      |
      |      initializationFailFast = true // This property controls whether the pool will "fail fast" if the pool cannot be seeded with initial connections successfully. If you want your application to start even when the database is down/unavailable, set this property to false. Default: true
      |
      |      keepAliveConnection = on // ensures that the database does not get dropped while we are using it
      |
      |      numThreads = 4 // number of cores
      |      maxConnections = 4  // same as numThreads
      |      minConnections = 4  // same as numThreads
      |    }
      |  }
      |}
      |
      |# the akka-persistence-query provider in use
      |jdbc-read-journal {
      |  class = "akka.persistence.jdbc.query.JdbcReadJournalProvider"
      |
      |  # New events are retrieved (polled) with this interval.
      |  refresh-interval = "300ms"
      |
      |  # How many events to fetch in one query (replay) and keep buffered until they
      |  # are delivered downstreams.
      |  max-buffer-size = "10"
      |
      |  dao = "akka.persistence.jdbc.dao.bytea.readjournal.ByteArrayReadJournalDao"
      |
      |  tables {
      |    journal {
      |      tableName = "journal"
      |      schemaName = ""
      |      columnNames {
      |        ordering = "ordering"
      |        persistenceId = "persistence_id"
      |        sequenceNumber = "sequence_number"
      |        created = "created"
      |        tags = "tags"
      |        message = "message"
      |      }
      |    }
      |  }
      |
      |  tagSeparator = ","
      |
      |  slick {
      |    profile = "slick.jdbc.OracleProfile$"
      |    db {
      |      host = "localhost"
      |      host = ${?POSTGRES_HOST}
      |      port = "5432"
      |      port = ${?POSTGRES_PORT}
      |      name = "docker"
      |
      |      url = "jdbc:postgresql://"${akka.persistence.jdbc.slick.db.host}":"${akka.persistence.jdbc.slick.db.port}"/"${akka.persistence.jdbc.slick.db.name}
      |      user = "docker"
      |      password = "docker"
      |      driver = "org.postgresql.Driver"
      |
      |      // hikariCP settings; see: https://github.com/brettwooldridge/HikariCP
      |
      |      // read: https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing
      |      // slick will use an async executor with a fixed size queue of 10.000 objects
      |      // The async executor is a connection pool for asynchronous execution of blocking I/O actions.
      |      // This is used for the asynchronous query execution API on top of blocking back-ends like JDBC.
      |      queueSize = 10000 // number of objects that can be queued by the async exector
      |
      |      connectionTimeout = 30000 // This property controls the maximum number of milliseconds that a client (that's you) will wait for a connection from the pool. If this time is exceeded without a connection becoming available, a SQLException will be thrown. 1000ms is the minimum value. Default: 30000 (30 seconds)
      |      validationTimeout = 5000 // This property controls the maximum amount of time that a connection will be tested for aliveness. This value must be less than the connectionTimeout. The lowest accepted validation timeout is 1000ms (1 second). Default: 5000
      |      idleTimeout = 600000 // 10 minutes: This property controls the maximum amount of time that a connection is allowed to sit idle in the pool. Whether a connection is retired as idle or not is subject to a maximum variation of +30 seconds, and average variation of +15 seconds. A connection will never be retired as idle before this timeout. A value of 0 means that idle connections are never removed from the pool. Default: 600000 (10 minutes)
      |      maxLifetime = 1800000 // 30 minutes: This property controls the maximum lifetime of a connection in the pool. When a connection reaches this timeout it will be retired from the pool, subject to a maximum variation of +30 seconds. An in-use connection will never be retired, only when it is closed will it then be removed. We strongly recommend setting this value, and it should be at least 30 seconds less than any database-level connection timeout. A value of 0 indicates no maximum lifetime (infinite lifetime), subject of course to the idleTimeout setting. Default: 1800000 (30 minutes)
      |      leakDetectionThreshold = 0 // This property controls the amount of time that a connection can be out of the pool before a message is logged indicating a possible connection leak. A value of 0 means leak detection is disabled. Lowest acceptable value for enabling leak detection is 2000 (2 secs). Default: 0
      |
      |      initializationFailFast = true // This property controls whether the pool will "fail fast" if the pool cannot be seeded with initial connections successfully. If you want your application to start even when the database is down/unavailable, set this property to false. Default: true
      |
      |      keepAliveConnection = on // ensures that the database does not get dropped while we are using it
      |
      |      numThreads = 4 // number of cores
      |      maxConnections = 4  // same as numThreads
      |      minConnections = 4  // same as numThreads
      |    }
      |  }
      |}
    """.stripMargin)
    .withFallback(referenceConfig)
    .resolve()

  "reference config" should "parse JournalConfig" in {
    val cfg = new JournalConfig(referenceConfig.getConfig("jdbc-journal"))
    val slickConfiguration = new SlickConfiguration(referenceConfig.getConfig("jdbc-journal.slick"))
    slickConfiguration.jndiName shouldBe None
    slickConfiguration.jndiDbName shouldBe None

    cfg.pluginConfig.dao shouldBe "akka.persistence.jdbc.journal.dao.DefaultJournalDao"
    cfg.pluginConfig.tagSeparator shouldBe ","

    cfg.journalTableConfiguration.tableName shouldBe "journal"
    cfg.journalTableConfiguration.schemaName shouldBe None

    cfg.journalTableConfiguration.columnNames.ordering shouldBe "ordering"
    cfg.journalTableConfiguration.columnNames.created shouldBe "created"
    cfg.journalTableConfiguration.columnNames.message shouldBe "message"
    cfg.journalTableConfiguration.columnNames.persistenceId shouldBe "persistence_id"
    cfg.journalTableConfiguration.columnNames.sequenceNumber shouldBe "sequence_number"
    cfg.journalTableConfiguration.columnNames.tags shouldBe "tags"

    cfg.daoConfig.logicalDelete shouldBe true
  }

  it should "parse SnapshotConfig" in {
    val cfg = new SnapshotConfig(referenceConfig.getConfig("jdbc-snapshot-store"))
    val slickConfiguration = new SlickConfiguration(referenceConfig.getConfig("jdbc-journal.slick"))
    slickConfiguration.jndiName shouldBe None
    slickConfiguration.jndiDbName shouldBe None

    cfg.pluginConfig.dao shouldBe "akka.persistence.jdbc.snapshot.dao.DefaultSnapshotDao"

    cfg.legacySnapshotTableConfiguration.tableName shouldBe "snapshot"
    cfg.legacySnapshotTableConfiguration.schemaName shouldBe None

    cfg.legacySnapshotTableConfiguration.columnNames.persistenceId shouldBe "persistence_id"
    cfg.legacySnapshotTableConfiguration.columnNames.created shouldBe "created"
    cfg.legacySnapshotTableConfiguration.columnNames.sequenceNumber shouldBe "sequence_number"
    cfg.legacySnapshotTableConfiguration.columnNames.snapshot shouldBe "snapshot"
  }

  it should "parse ReadJournalConfig" in {
    val cfg = new ReadJournalConfig(referenceConfig.getConfig("jdbc-read-journal"))
    val slickConfiguration = new SlickConfiguration(referenceConfig.getConfig("jdbc-journal.slick"))
    slickConfiguration.jndiName shouldBe None
    slickConfiguration.jndiDbName shouldBe None

    cfg.pluginConfig.dao shouldBe "akka.persistence.jdbc.query.dao.DefaultReadJournalDao"
    cfg.pluginConfig.tagSeparator shouldBe ","
    cfg.refreshInterval shouldBe 1.second
    cfg.maxBufferSize shouldBe 500

    cfg.journalTableConfiguration.tableName shouldBe "journal"
    cfg.journalTableConfiguration.schemaName shouldBe None

    cfg.journalTableConfiguration.columnNames.ordering shouldBe "ordering"
    cfg.journalTableConfiguration.columnNames.created shouldBe "created"
    cfg.journalTableConfiguration.columnNames.message shouldBe "message"
    cfg.journalTableConfiguration.columnNames.persistenceId shouldBe "persistence_id"
    cfg.journalTableConfiguration.columnNames.sequenceNumber shouldBe "sequence_number"
    cfg.journalTableConfiguration.columnNames.tags shouldBe "tags"
  }

  "full config" should "parse JournalConfig" in {
    val cfg = new JournalConfig(config.getConfig("jdbc-journal"))
    val slickConfiguration = new SlickConfiguration(config.getConfig("jdbc-journal.slick"))
    slickConfiguration.jndiName shouldBe None
    slickConfiguration.jndiDbName shouldBe None

    cfg.pluginConfig.dao shouldBe "akka.persistence.jdbc.dao.bytea.journal.ByteArrayJournalDao"
    cfg.pluginConfig.tagSeparator shouldBe ","

    cfg.journalTableConfiguration.tableName shouldBe "journal"
    cfg.journalTableConfiguration.schemaName shouldBe None

    cfg.journalTableConfiguration.columnNames.ordering shouldBe "ordering"
    cfg.journalTableConfiguration.columnNames.created shouldBe "created"
    cfg.journalTableConfiguration.columnNames.message shouldBe "message"
    cfg.journalTableConfiguration.columnNames.persistenceId shouldBe "persistence_id"
    cfg.journalTableConfiguration.columnNames.sequenceNumber shouldBe "sequence_number"
    cfg.journalTableConfiguration.columnNames.tags shouldBe "tags"
  }

  it should "parse SnapshotConfig" in {
    val cfg = new SnapshotConfig(config.getConfig("jdbc-snapshot-store"))
    val slickConfiguration = new SlickConfiguration(config.getConfig("jdbc-snapshot-store.slick"))
    slickConfiguration.jndiName shouldBe None
    slickConfiguration.jndiDbName shouldBe None

    cfg.pluginConfig.dao shouldBe "akka.persistence.jdbc.dao.bytea.snapshot.ByteArraySnapshotDao"

    cfg.legacySnapshotTableConfiguration.tableName shouldBe "snapshot"
    cfg.legacySnapshotTableConfiguration.schemaName shouldBe None
    cfg.legacySnapshotTableConfiguration.columnNames.persistenceId shouldBe "persistence_id"
    cfg.legacySnapshotTableConfiguration.columnNames.created shouldBe "created"
    cfg.legacySnapshotTableConfiguration.columnNames.sequenceNumber shouldBe "sequence_number"
    cfg.legacySnapshotTableConfiguration.columnNames.snapshot shouldBe "snapshot"
  }

  it should "parse ReadJournalConfig" in {
    val cfg = new ReadJournalConfig(config.getConfig("jdbc-read-journal"))
    val slickConfiguration = new SlickConfiguration(config.getConfig("jdbc-read-journal.slick"))
    slickConfiguration.jndiName shouldBe None
    slickConfiguration.jndiDbName shouldBe None

    cfg.pluginConfig.dao shouldBe "akka.persistence.jdbc.dao.bytea.readjournal.ByteArrayReadJournalDao"
    cfg.pluginConfig.tagSeparator shouldBe ","
    cfg.refreshInterval shouldBe 300.millis
    cfg.maxBufferSize shouldBe 10

    cfg.journalTableConfiguration.tableName shouldBe "journal"
    cfg.journalTableConfiguration.schemaName shouldBe None

    cfg.journalTableConfiguration.columnNames.ordering shouldBe "ordering"
    cfg.journalTableConfiguration.columnNames.created shouldBe "created"
    cfg.journalTableConfiguration.columnNames.message shouldBe "message"
    cfg.journalTableConfiguration.columnNames.persistenceId shouldBe "persistence_id"
    cfg.journalTableConfiguration.columnNames.sequenceNumber shouldBe "sequence_number"
    cfg.journalTableConfiguration.columnNames.tags shouldBe "tags"
  }
}
