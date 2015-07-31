package com.collective.modelmatrix.db

import org.flywaydb.core.Flyway

trait SchemaInstaller {
  val dbConfig: DatabaseConfig

  def installOrMigrate(): Boolean = {
    val flyway = new Flyway
    flyway.setBaselineOnMigrate(true)
    flyway.setBaselineVersionAsString("000")
    flyway.setTable("mmc_schema_version")
    flyway.setDataSource(dbConfig.dbUrl, null, null)
    flyway.setSqlMigrationPrefix("mmc")
    flyway.setLocations(dbConfig.migrationPath)
    val appliedMigrations = flyway.migrate()

    Console.println(s"Schema successfully installed. Applied migrations: $appliedMigrations")
    return true
  }
}
