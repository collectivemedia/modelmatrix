package com.collective.modelmatrix.db

import com.typesafe.config.{Config, ConfigFactory}
import slick.driver.{H2Driver, JdbcProfile, PostgresDriver}


// database configuration wrapper class that read the configuration files and
// determines the proper slick driver to use
class DatabaseConfigWrapper(configFilePath: String = "") {
  // Differentiate between the different types of database supported
  private [DatabaseConfigWrapper] case class DatabaseConfig(val name: String,
                                                            val driverClass: String,
                                                            val slickDriver: JdbcProfile)

  private[this] val PG = DatabaseConfig("pg", "org.postgresql.Driver", PostgresDriver)
  private[this] val H2 = DatabaseConfig("h2", "org.h2.Driver", H2Driver)

  private[this] val dbConfigPath: String = "modelmatrix.catalog.db"

  lazy val currentDB: DatabaseConfig = getCurrentDB
  lazy val dbConfig: Config =
    if (configFilePath.isEmpty) ConfigFactory.load().getConfig(dbConfigPath)
    else ConfigFactory.systemProperties().withFallback(ConfigFactory.load(configFilePath)).getConfig(dbConfigPath)

  // do not need to expose this as user have access to the current db config using attribute currentDB
  private[this] def getCurrentDB: DatabaseConfig = {
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


