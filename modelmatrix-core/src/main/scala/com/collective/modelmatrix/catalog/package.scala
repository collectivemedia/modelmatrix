package com.collective.modelmatrix

import java.sql.Timestamp
import java.time.Instant

import slick.driver.PostgresDriver.api._

package object catalog {

  implicit val instantColumnType =
    MappedColumnType.base[Instant, java.sql.Timestamp](
      instant => Timestamp.from(instant),
      _.toInstant
    )

}
