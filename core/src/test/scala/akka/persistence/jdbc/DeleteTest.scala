package akka.persistence.jdbc

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.persistence.{DeleteMessagesSuccess, PersistentActor}
import akka.persistence.jdbc.query.scaladsl.JdbcReadJournal
import akka.persistence.jdbc.testkit.javadsl.SchemaUtils
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

object DeleteTest {
  val config: String =
    """
      |akka.actor.allow-java-serialization = on
      |akka {
      |  persistence {
      |    journal {
      |      plugin = "jdbc-journal"
      |      // Enable the line below to automatically start the journal when the actorsystem is started
      |      // auto-start-journals = ["jdbc-journal"]
      |    }
      |    snapshot-store {
      |      plugin = "jdbc-snapshot-store"
      |      // Enable the line below to automatically start the snapshot-store when the actorsystem is started
      |      // auto-start-snapshot-stores = ["jdbc-snapshot-store"]
      |    }
      |  }
      |}
      |
      |jdbc-journal {
      |  slick = ${slick}
      |}
      |
      |# the akka-persistence-snapshot-store in use
      |jdbc-snapshot-store {
      |  slick = ${slick}
      |}
      |
      |# the akka-persistence-query provider in use
      |jdbc-read-journal {
      |  slick = ${slick}
      |}
      |
      |slick {
      |  profile = "slick.jdbc.H2Profile$"
      |  db {
      |    url = "jdbc:h2:mem:test-database;DATABASE_TO_UPPER=false;"
      |    user = "root"
      |    password = "root"
      |    driver = "org.h2.Driver"
      |    numThreads = 5
      |    maxConnections = 5
      |    minConnections = 1
      |  }
      |}
      |""".stripMargin


  case class Persist(value: String)
  case class Delete(toSeq: Long)
  case class Event(value: String)
  case object AckPersist
  case class AckDelete(toSeq: Long)

  class PersistingActor extends PersistentActor {
    private var deleteSender: ActorRef = _

    override def persistenceId: String = "testId"

    override def receiveRecover: Receive = {
      case _ =>
    }

    override def receiveCommand: Receive = {
      case Persist(value) =>
        persist(Event(value)) { _ =>
          sender() ! AckPersist
        }

      case Delete(toSeq) =>
        deleteMessages(toSeq)
        deleteSender = sender()

      case m: DeleteMessagesSuccess =>
        deleteSender ! AckDelete(m.toSequenceNr)
    }
  }
}

class DeleteTest private(_system: ActorSystem) extends TestKit(_system) with AnyFlatSpecLike with Matchers {
  import DeleteTest._

  def this() = this(
    ActorSystem("h2test",
      ConfigFactory.systemProperties()
          .withFallback(ConfigFactory.parseString(DeleteTest.config))
          .withFallback(ConfigFactory.load())
          .resolve())
  )

  it should "delete events correctly" in {
    SchemaUtils.createIfNotExists(system)

    val probe = TestProbe()
    val persister = system.actorOf(Props(new PersistingActor()))

    probe.send(persister, Persist("1"))
    probe.expectMsg(AckPersist)
    probe.send(persister, Persist("2"))
    probe.expectMsg(AckPersist)
    probe.send(persister, Persist("3"))
    probe.expectMsg(AckPersist)

    probe.send(persister, Delete(2))
    probe.expectMsg(AckDelete(2)) // should delete

    val eventProbe = TestProbe()
    PersistenceQuery(system).readJournalFor[JdbcReadJournal]("jdbc-read-journal").eventsByPersistenceId("testId", 0, Long.MaxValue).runForeach(e => eventProbe.ref ! e)
    val event = eventProbe.expectMsgType[EventEnvelope]
    event.sequenceNr shouldBe 3 // succeeds with Cassandra or LevelDB, with H2 event seqNr 2 is returned
  }
}
