package akka.persistence.jdbc.state

import akka.persistence.jdbc.config.DurableStateTableConfiguration

object DurableStateTables {
  case class DurableStateRow(
      globalOffset: Long,
      persistenceId: String,
      seqNumber: Long,
      statePayload: Array[Byte],
      tag: Option[String],
      stateSerId: Int,
      stateSerManifest: Option[String],
      stateTimestamp: Long)
}

trait DurableStateTables {
  val profile: slick.jdbc.JdbcProfile
  import profile.api._
  def durableStateTableCfg: DurableStateTableConfiguration

  import DurableStateTables._

  class DurableState(_tableTag: Tag)
      extends Table[DurableStateRow](
        _tableTag,
        _schemaName = durableStateTableCfg.schemaName,
        _tableName = durableStateTableCfg.tableName) {

    def * =
      (globalOffset, persistenceId, seqNumber, statePayload, tag, stateSerId, stateSerManifest, stateTimestamp)
        .<>(DurableStateRow.tupled, DurableStateRow.unapply)

    val globalOffset: Rep[Long] = column[Long](durableStateTableCfg.columnNames.globalOffset, O.AutoInc)
    val persistenceId: Rep[String] =
      column[String](durableStateTableCfg.columnNames.persistenceId, O.PrimaryKey, O.Length(255, varying = true))
    val seqNumber: Rep[Long] = column[Long](durableStateTableCfg.columnNames.seqNumber)
    val statePayload: Rep[Array[Byte]] = column[Array[Byte]](durableStateTableCfg.columnNames.statePayload)
    val tag: Rep[Option[String]] = column[Option[String]](durableStateTableCfg.columnNames.tag)
    val stateSerId: Rep[Int] = column[Int](durableStateTableCfg.columnNames.stateSerId)
    val stateSerManifest: Rep[Option[String]] =
      column[Option[String]](durableStateTableCfg.columnNames.stateSerManifest)
    val stateTimestamp: Rep[Long] = column[Long](durableStateTableCfg.columnNames.stateTimestamp)

    val globalOffsetIdx = index(s"${tableName}_globalOffset_idx", globalOffset, unique = true)
  }
  lazy val durableStateTable = new TableQuery(new DurableState(_))
}
