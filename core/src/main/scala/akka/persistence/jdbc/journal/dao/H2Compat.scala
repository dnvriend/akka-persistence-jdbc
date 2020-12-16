package akka.persistence.jdbc.journal.dao

import slick.jdbc.JdbcProfile

trait H2Compat {

  val profile: JdbcProfile

  private lazy val isH2Driver = profile match {
    case slick.jdbc.H2Profile => true
    case _                    => false
  }

  def correctMaxForH2Driver(max: Long): Long = {
    if (isH2Driver) {
      Math.min(max, Int.MaxValue) // H2 only accepts a LIMIT clause as an Integer
    } else {
      max
    }
  }
}
