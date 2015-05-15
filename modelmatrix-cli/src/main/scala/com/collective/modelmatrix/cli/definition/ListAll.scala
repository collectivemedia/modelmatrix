package com.collective.modelmatrix.cli.definition

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scalaz.@@

case class ListAll(
  dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends ListDefinitions {

  def run(): Unit = {
    printDefinitions(blockOn(db.run(modelDefinitions.all)))
  }
}
