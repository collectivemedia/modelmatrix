package com.collective.modelmatrix.catalog

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import slick.driver.H2Driver

trait PgDatabase extends TestDatabase {
  self: FlatSpec with BeforeAndAfterAll =>

  val driver = H2Driver

  import driver.api._

  val db = Database.forConfig("pgdev", ConfigFactory.load("./pg.conf"))

  override protected def afterAll(): Unit = {
    db.close()
  }

}
