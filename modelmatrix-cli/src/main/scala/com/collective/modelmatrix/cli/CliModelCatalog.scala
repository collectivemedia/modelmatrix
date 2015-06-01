package com.collective.modelmatrix.cli

import com.collective.modelmatrix.catalog._
import com.typesafe.config.Config
import slick.driver.PostgresDriver

import scala.concurrent.ExecutionContext
import scalaz._

trait CliModelCatalog {

  protected val driver = slick.driver.PostgresDriver
  import driver.api._

  def dbName: String
  def dbConfig: Config
  implicit def ec: ExecutionContext @@ ModelMatrixCatalog

  protected lazy val db = Database.forConfig(dbName, dbConfig)
  protected lazy val catalog = new ModelMatrixCatalog(PostgresDriver)
  
  protected lazy val modelDefinitions = new ModelDefinitions(catalog)
  protected lazy val modelDefinitionFeatures = new ModelDefinitionFeatures(catalog)
  
  protected lazy val modelInstances = new ModelInstances(catalog)
  protected lazy val modelInstanceFeatures = new ModelInstanceFeatures(catalog)
}
