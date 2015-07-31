package com.collective.modelmatrix.db

import com.collective.modelmatrix.db.DatabaseConfig.{DatabaseType, H2, PG}
import com.typesafe.config.{Config, ConfigFactory}
import slick.driver.{JdbcDriver, H2Driver, JdbcProfile, PostgresDriver}

object GenericSlickDriver extends JdbcDriver

object DatabaseConfig {

  val driverPath = "driver"
  val urlPath = "url"

  case class DatabaseType(name: String,
                          driverClass: String,
                          slickDriver: JdbcProfile,
                          urlPrefix: String)

  object PG extends DatabaseType("pg", "org.postgresql.Driver", PostgresDriver, "jdbc:postgresql:")

  object H2 extends DatabaseType("h2", "org.h2.Driver", H2Driver, "jdbc:h2:")

}

// database configuration wrapper class that read the configuration files and
// determines the proper slick driver to use
class DatabaseConfig(configFilePath: String = "") {
  private[this] val dbConfigPath: String = "modelmatrix.catalog.db"

  private[this] lazy val dbConfig: Config =
    if (configFilePath.isEmpty) ConfigFactory.load().getConfig(dbConfigPath)
    else ConfigFactory.systemProperties().withFallback(ConfigFactory.load(configFilePath)).getConfig(dbConfigPath)

  // get the DatabaseType based on the driver name
  private[this] lazy val dbType: DatabaseType =
    (dbConfig.getString(DatabaseConfig.driverPath), dbConfig.getString(DatabaseConfig.urlPath)) match {
      case pg if PG.driverClass == pg._1 && pg._2.startsWith(PG.urlPrefix) => PG
      case h2 if H2.driverClass == h2._1 && h2._2.startsWith(H2.urlPrefix) => H2
      case unknown => sys.error(s"The following db driver '${unknown._1}' with url '${unknown._2}' is not supported")
    }

  lazy val dbUrl = dbConfig.getString(DatabaseConfig.urlPath)

  def slickDriver: JdbcProfile = {
    dbType.slickDriver
  }

  def database(): GenericSlickDriver.api.Database = {
    import GenericSlickDriver.api.Database
    Database.forConfig("", dbConfig)
  }

  def migrationPath: String = {
    "db/migration/%s".format(dbType.name)
  }
}

// default database configuration wrapper that will be used in production
object DefaultDatabaseConfig extends DatabaseConfig


