package com.collective.modelmatrix.cli.db

import com.collective.modelmatrix.ModelMatrix.DatabaseConfigWrapper
import com.collective.modelmatrix.cli.Script
import com.typesafe.config.ConfigFactory
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

case class InstallSchema() extends Script {

  private val log = LoggerFactory.getLogger(classOf[InstallSchema])

  override def run(): Unit = {
    log.info(s"Install Model Matrix catalog schema")

    val config = ConfigFactory.load().getConfig("modelmatrix.catalog.db")
    val url = config.getString("url")

    val flyway = new Flyway
    flyway.setBaselineOnMigrate(true)
    flyway.setBaselineVersionAsString("000")
    flyway.setTable("mmc_schema_version")
    flyway.setDataSource(url, null, null)
    flyway.setSqlMigrationPrefix("v")
    flyway.setLocations(DatabaseConfigWrapper.getMigrationPath)
    val appliedMigrations = flyway.migrate()

    Console.println(s"Schema successfully installed. Applied migrations: $appliedMigrations")
  }

}
