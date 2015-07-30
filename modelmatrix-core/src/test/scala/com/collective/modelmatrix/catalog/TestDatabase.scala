package com.collective.modelmatrix.catalog

import com.collective.modelmatrix.db.DatabaseConfig
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

trait ITDatabase extends CatalogDatabase {
  self: FlatSpec with BeforeAndAfterAll =>
  val dbConfig: DatabaseConfig = ITDatabaseConfig
  override val db = dbConfig.database()
  override val driver = dbConfig.slickDriver
}

trait TestDatabase extends CatalogDatabase {
  self: FlatSpec with BeforeAndAfterAll =>
  val dbConfig: DatabaseConfig = TestDatabaseConfig
  override val db = dbConfig.database()
  override val driver = dbConfig.slickDriver
}

object ITDatabaseConfig extends DatabaseConfig("reference.it.conf")
object TestDatabaseConfig extends DatabaseConfig("reference.test.conf")


