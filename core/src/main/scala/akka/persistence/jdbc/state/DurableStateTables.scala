package akka.persistence.jdbc.state

import akka.persistence.jdbc.config.DurableStateTableConfiguration

object DurableStateTables {
  case class DurableStateRow(
      persistenceId: String,
      statePayload: Array[Byte],
      seqNumber: Long,
      stateSerId: Int,
      stateSerManifest: Option[String])
}

trait DurableStateTables {
  val profile: slick.jdbc.JdbcProfile
  import profile.api._
  def durableStateTableCfg: DurableStateTableConfiguration
  // TODO:
  // def tagTableCfg: DurableStateTagTableConfiguration

  import DurableStateTables._

  class DurableState(_tableTag: Tag)
      extends Table[DurableStateRow](
        _tableTag,
        _schemaName = durableStateTableCfg.schemaName,
        _tableName = durableStateTableCfg.tableName) {

    def * =
      (persistenceId, statePayload, seqNumber, stateSerId, stateSerManifest)
        .<>(DurableStateRow.tupled, DurableStateRow.unapply)

    val persistenceId: Rep[String] =
      column[String](durableStateTableCfg.columnNames.persistenceId, O.PrimaryKey, O.Length(255, varying = true))
    val statePayload: Rep[Array[Byte]] = column[Array[Byte]](durableStateTableCfg.columnNames.statePayload)
    val seqNumber: Rep[Long] = column[Long](durableStateTableCfg.columnNames.seqNumber)
    val stateSerId: Rep[Int] = column[Int](durableStateTableCfg.columnNames.stateSerId)
    val stateSerManifest: Rep[Option[String]] = column[Option[String]](durableStateTableCfg.columnNames.stateSerManifest)
  }
  lazy val durableStateTable = new TableQuery(new DurableState(_))
}
