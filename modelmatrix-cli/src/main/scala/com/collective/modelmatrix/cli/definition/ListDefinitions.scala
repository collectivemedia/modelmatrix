package com.collective.modelmatrix.cli.definition

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{Script, CliModelCatalog}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz.@@

case class ListDefinitions(
  name: Option[String], dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[ListDefinitions])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"List Model Matrix definitions. " +
      s"Name: ${name.getOrElse("-")}. " +
      s"Database: $dbName @ ${dbConfig.origin()}")
    blockOn(db.run(modelDefinitions.list(name))).printASCIITable()
  }
}
