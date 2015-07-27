package com.collective.modelmatrix.catalog


import com.collective.modelmatrix.db.DatabaseConfigWrapper
import org.scalatest.{BeforeAndAfterAll, FlatSpec}


trait PgDatabaseIT extends CatalogDatabase {
  self: FlatSpec with BeforeAndAfterAll =>

  val dbConfigWrapper = PgConfigWrapperIT
  lazy val driver = dbConfigWrapper.getSlickDriver
  override val db = dbConfigWrapper.getDatabase

  override protected def afterAll(): Unit = {
    db.close()
  }
}

object PgConfigWrapperIT extends DatabaseConfigWrapper("modelmatrix.catalog.db.pg", "./database_it.conf")