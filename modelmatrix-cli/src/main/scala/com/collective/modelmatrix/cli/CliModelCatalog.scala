package com.collective.modelmatrix.cli

import com.collective.modelmatrix.catalog._
import com.typesafe.config.ConfigFactory
import slick.driver.PostgresDriver

import scala.concurrent.ExecutionContext
import scalaz._

trait CliModelCatalog {

  protected val driver = slick.driver.PostgresDriver
  import driver.api._

  implicit def ec: ExecutionContext @@ ModelMatrixCatalog

  protected lazy val db = Database.forConfig("modelmatrix.catalog.db", ConfigFactory.load())
  protected lazy val catalog = new ModelMatrixCatalog(PostgresDriver)

  protected lazy val modelDefinitions = new ModelDefinitions(catalog)
  protected lazy val modelDefinitionFeatures = new ModelDefinitionFeatures(catalog)

  protected lazy val modelInstances = new ModelInstances(catalog)
  protected lazy val modelInstanceFeatures = new ModelInstanceFeatures(catalog)
}
