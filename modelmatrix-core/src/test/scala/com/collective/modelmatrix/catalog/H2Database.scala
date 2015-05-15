package com.collective.modelmatrix.catalog

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import slick.driver.H2Driver

import scala.concurrent.Await
import scala.concurrent.duration._

// Database shared between tests
object H2Database {
  import H2Driver.api._

  private[this] var schemaInstalled: Boolean = false

  private[H2Database] lazy val db =
    Database.forConfig("h2dev", ConfigFactory.load("./h2.conf"))

  private[H2Database] def installSchema(schema: Schema): Unit = this.synchronized {
    if (!schemaInstalled) {
      Await.result(db.run(DBIO.seq(schema.create)), 10.seconds)
      schemaInstalled = true
    }
  }

}

trait H2Database extends CatalogDatabase {
  self: FlatSpec with BeforeAndAfterAll =>

  val driver = H2Driver

  val db = H2Database.db

  override protected def beforeAll(): Unit = {
    H2Database.installSchema(schema)
  }
}
