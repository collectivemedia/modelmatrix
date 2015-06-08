package com.collective.modelmatrix.catalog

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import slick.driver.PostgresDriver

trait PgDatabase extends CatalogDatabase {
  self: FlatSpec with BeforeAndAfterAll =>

  val driver = PostgresDriver

  import driver.api._

  val db = Database.forConfig("pgdev", ConfigFactory.load("./pg.conf"))

  override protected def afterAll(): Unit = {
    db.close()
  }

}
