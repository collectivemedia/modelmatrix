package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{CliModelCatalog, Script}
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scalaz._

/**
 * List all Model Matrix instances in catalog
 */
case class List(
  dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  def run(): Unit = {
    printInstances(blockOn(db.run(modelInstances.all)))
  }
}
