package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{CliModelCatalog, Script}
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scalaz._

/**
 * Find Model Matrix instances by name in catalog
 */
case class Find(
  name: String, dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  def run(): Unit = {
    printInstances(blockOn(db.run(modelInstances.findByName(name))))
  }
}
