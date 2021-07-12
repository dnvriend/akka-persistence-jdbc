import sbt._
import Keys._

object Dependencies {
  val Nightly = sys.env.get("TRAVIS_EVENT_TYPE").contains("cron")

  // Keep in sync with .travis.yml
  val Scala212 = "2.12.13"
  val Scala213 = "2.13.6"
  val ScalaVersions = Seq(Scala212, Scala213)

  val AkkaVersion = "2.6.10"
  val AkkaBinaryVersion = AkkaVersion.take(3)

  val SlickVersion = "3.3.3"
  val ScalaTestVersion = "3.2.9"

  val JdbcDrivers = Seq(
    "org.postgresql" % "postgresql" % "42.2.22",
    "com.h2database" % "h2" % "1.4.200",
    "mysql" % "mysql-connector-java" % "8.0.25",
    "com.microsoft.sqlserver" % "mssql-jdbc" % "7.4.1.jre8")

  val Libraries: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion,
    "com.typesafe.slick" %% "slick" % SlickVersion,
    "com.typesafe.slick" %% "slick-hikaricp" % SlickVersion,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
    "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
    "org.scalatest" %% "scalatest" % ScalaTestVersion % Test) ++ JdbcDrivers.map(_ % Test)

  val Migration: Seq[ModuleID] = Seq(
    "org.flywaydb" % "flyway-core" % "7.10.0",
    "com.typesafe" % "config" % "1.4.1",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "org.testcontainers" % "postgresql" % "1.15.3" % Test,
    "org.scalatest" %% "scalatest" % ScalaTestVersion % Test) ++ JdbcDrivers.map(_ % Provided)
}
