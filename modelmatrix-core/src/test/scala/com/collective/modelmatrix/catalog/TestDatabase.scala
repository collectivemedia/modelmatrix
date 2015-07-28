package com.collective.modelmatrix.catalog

import com.collective.modelmatrix.db.DatabaseConfigWrapper
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

trait ITDatabase extends CatalogDatabase {
  self: FlatSpec with BeforeAndAfterAll =>

  val dbConfigWrapper: DatabaseConfigWrapper = ITConfigWrapper
  val driver = dbConfigWrapper.getSlickDriver
  override val db = dbConfigWrapper.getDatabase
}

trait TestDatabase extends CatalogDatabase {
  self: FlatSpec with BeforeAndAfterAll =>

  val dbConfigWrapper: DatabaseConfigWrapper = TestConfigWrapper
  val driver = dbConfigWrapper.getSlickDriver
  override val db = dbConfigWrapper.getDatabase
}

object ITConfigWrapper extends DatabaseConfigWrapper("./database_it.conf")
object TestConfigWrapper extends DatabaseConfigWrapper("./database_test.conf")


