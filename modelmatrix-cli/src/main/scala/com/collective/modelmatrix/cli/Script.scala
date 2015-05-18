package com.collective.modelmatrix.cli

import com.collective.modelmatrix.catalog.{ModelDefinitionFeatures, ModelDefinitions, ModelMatrixCatalog}
import com.typesafe.config.Config
import scopt.OptionParser
import slick.driver.PostgresDriver
import slick.driver.PostgresDriver.api._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scalaz._


trait Script {

  def run(): Unit

  protected def blockOn[T](f: Future[T], duration: FiniteDuration = 10.seconds) = {
    Await.result(f, duration)
  }

}

trait ModelCatalogScript extends Script {

  def dbName: String
  def dbConfig: Config
  implicit def ec: ExecutionContext @@ ModelMatrixCatalog

  protected lazy val db = Database.forConfig(dbName, dbConfig)
  protected lazy val schema = new ModelMatrixCatalog(PostgresDriver)
  protected lazy val modelDefinitions = new ModelDefinitions(schema)
  protected lazy val modelDefinitionFeatures = new ModelDefinitionFeatures(schema)
}

object Script {
  def noOp[T](parser: OptionParser[T]): Script = new Script {
    def run(): Unit = {
      parser.showUsageAsError
    }
  }
}
