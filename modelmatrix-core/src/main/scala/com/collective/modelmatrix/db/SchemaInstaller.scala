package com.collective.modelmatrix.db

import org.flywaydb.core.Flyway

/**
 * Created by jpocalan on 7/27/15.
 */
trait SchemaInstaller {
  val dbConfigWrapper: DatabaseConfigWrapper

  def installOrMigrate(): Boolean = {
    val config = dbConfigWrapper.dbConfig
    val url = config.getString("url")

    val flyway = new Flyway
    flyway.setBaselineOnMigrate(true)
    flyway.setBaselineVersionAsString("000")
    flyway.setTable("mmc_schema_version")
    flyway.setDataSource(url, null, null)
    flyway.setSqlMigrationPrefix("v")
    flyway.setLocations(dbConfigWrapper.getMigrationPath)
    val appliedMigrations = flyway.migrate()

    Console.println(s"Schema successfully installed. Applied migrations: $appliedMigrations")
    return true
  }
}
