package com.collective.modelmatrix.catalog

import com.collective.modelmatrix.db.DatabaseConfigWrapper
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

trait H2Database extends CatalogDatabase {
  self: FlatSpec with BeforeAndAfterAll =>

  val dbConfigWrapper: DatabaseConfigWrapper = H2ConfigWrapper
  val driver = dbConfigWrapper.getSlickDriver
  override val db = dbConfigWrapper.getDatabase
}

trait H2DatabaseIT extends CatalogDatabase {
  self: FlatSpec with BeforeAndAfterAll =>

  val dbConfigWrapper: DatabaseConfigWrapper = H2ConfigWrapperIT
  val driver = dbConfigWrapper.getSlickDriver
  override val db = dbConfigWrapper.getDatabase
}

object H2ConfigWrapperIT extends DatabaseConfigWrapper("modelmatrix.catalog.db.h2", "./database_it.conf")
object H2ConfigWrapper extends DatabaseConfigWrapper("modelmatrix.catalog.db.h2", "./database_test.conf")


