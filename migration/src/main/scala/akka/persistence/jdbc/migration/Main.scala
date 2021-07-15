/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.jdbc.migration

import com.typesafe.config.{ Config, ConfigFactory }
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.Location

object Main extends App {

  val config = ConfigFactory.load().getConfig("akka.persistence.jdbc.migration")

  def run(config: Config): Unit = {
    val vendor = config.getString("database-vendor")
    val url = config.getString("url")
    val user = config.getString("user")
    val password = config.getString("password")

    val flywayConfig = Flyway.configure.dataSource(url, user, password).table("apjdbc_schema_history")

    vendor match {
      case "postgres" =>
        flywayConfig.locations(new Location("classpath:db/migration/postgres"))
      case other =>
        sys.error(s"Akka Persistence JDBC migrations do not support `$other` (supported are `postgres`)")
    }

    val flyway = flywayConfig.load
    flyway.baseline()
    flyway.migrate()

  }
}
