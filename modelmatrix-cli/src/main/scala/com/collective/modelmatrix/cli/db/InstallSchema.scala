package com.collective.modelmatrix.cli.db

import com.collective.modelmatrix.cli.Script
import com.collective.modelmatrix.db.{DefaultDatabaseConfig, DatabaseConfig, SchemaInstaller}
import org.slf4j.LoggerFactory

case class InstallSchema() extends Script with SchemaInstaller{

  private val log = LoggerFactory.getLogger(classOf[InstallSchema])

  override def run(): Unit = {
    log.info(s"Install Model Matrix catalog schema")
    this.installOrMigrate()
  }

  override val dbConfig: DatabaseConfig = DefaultDatabaseConfig
}
