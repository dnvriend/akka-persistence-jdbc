/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.jdbc

import akka.persistence.jdbc.config._
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

abstract class TablesTestSpec extends AnyFlatSpec with Matchers {
  def toColumnName[A](tableName: String)(columnName: String): String = s"$tableName.$columnName"

  val config = ConfigFactory
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
      |        persistenceId = "persistence_id"
      |        sequenceNumber = "sequence_number"
      |        created = "created"
      |        tags = "tags"
      |        message = "message"
      |      }
      |    }
      |
      |    deletedTo {
      |      tableName = "deleted_to"
      |      schemaName = ""
      |      columnNames = {
      |        persistenceId = "persistence_id"
      |        deletedTo = "deleted_to"
      |      }
      |    }
      |  }
      |
      |  tagSeparator = ","
      |
      |  serialization = on // alter only when using a custom dao
      |
      |  dao = "akka.persistence.jdbc.dao.bytea.ByteArrayJournalDao"
      |
      |  slick {
      |    profile = "slick.jdbc.PostgresProfile"
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
      |  serialization = on // alter only when using a custom dao
      |
      |  dao = "akka.persistence.jdbc.dao.bytea.ByteArraySnapshotDao"
      |
      |  slick {
      |    profile = "slick.jdbc.PostgresProfile"
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
      |  refresh-interval = "1s"
      |
      |  # How many events to fetch in one query (replay) and keep buffered until they
      |  # are delivered downstreams.
      |  max-buffer-size = "500"
      |
      |  serialization = on // alter only when using a custom dao
      |
      |  dao = "akka.persistence.jdbc.dao.bytea.ByteArrayJournalDao"
      |
      |  tables {
      |    journal {
      |      tableName = "journal"
      |      schemaName = ""
      |      columnNames {
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
      |    profile = "slick.jdbc.PostgresProfile"
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
    .withFallback(ConfigFactory.load("reference"))
    .resolve()

  val journalConfig = new JournalConfig(config.getConfig("jdbc-journal"))
  val snapshotConfig = new SnapshotConfig(config.getConfig("jdbc-snapshot-store"))
  val readJournalConfig = new ReadJournalConfig(config.getConfig("jdbc-read-journal"))
}
