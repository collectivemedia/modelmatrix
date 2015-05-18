package com.collective.modelmatrix.cli.definition

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.ModelCatalogScript
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scalaz._

/**
 * Find Model Matrix definitions by name in catalog
 */
case class Find(
  name: String, dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends ModelCatalogScript {

  def run(): Unit = {
    printDefinitions(blockOn(db.run(modelDefinitions.findByName(name))))
  }
}
