package com.collective.modelmatrix.cli.definition

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{Script, CliModelCatalog}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz.@@

case class ListDefinitions(
  dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[ListDefinitions])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"View all Model Matrix definitions. Database: $dbName @ ${dbConfig.origin()}")
    blockOn(db.run(modelDefinitions.all)).printASCIITable()
  }
}
