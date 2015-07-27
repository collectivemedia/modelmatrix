package com.collective.modelmatrix.db

import com.typesafe.config.{Config, ConfigFactory}
import slick.driver.{H2Driver, JdbcProfile, PostgresDriver}

/**
 * Created by jpocalan on 7/27/15.
 */
// TODO: replace by some sort of enum
// Differentiate between the different types of databse supported
class DatabaseConfig(n: String, dc: String, sd: JdbcProfile) {
  val name = n;
  val driverClass = dc;
  val slickDriver = sd;
}

// database configuration wrapper class that read the configuration files and
// determines the proper slick driver to use
case class DatabaseConfigWrapper(configPath: String = "modelmatrix.catalog.db", configFilePath: String = "") {
  val PG = new DatabaseConfig("pg", "org.postgresql.Driver", PostgresDriver)
  val H2 = new DatabaseConfig("h2", "org.h2.Driver", H2Driver)
  lazy val currentDB: DatabaseConfig = getCurrentDB

  val dbConfigPath: String = configPath
  lazy val dbConfig: Config =
    if (configFilePath.isEmpty) ConfigFactory.load().getConfig(dbConfigPath)
    else ConfigFactory.load(configFilePath).getConfig(dbConfigPath)

  // do not need to expose this as user have access to the current db config using attribute currentDB
  private def getCurrentDB = {
    dbConfig.getString("driver") match {
      case PG.driverClass => PG
      case H2.driverClass => H2
      case _ => throw new RuntimeException("The following db driver '%s' is not supported"
        .format(dbConfig.getString("driver")))
    }
  }

  def getSlickDriver: JdbcProfile = {
    currentDB.slickDriver
  }

  def getDatabase: currentDB.slickDriver.api.Database = {
    import currentDB.slickDriver.api._
    Database.forConfig("", dbConfig)
  }

  def getMigrationPath: String = {
    "db/migration/%s".format(currentDB.name)
  }
}

// default database configuration wrapper that will be used in production
object DefaultDBConfigWrapper extends DatabaseConfigWrapper


