package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{CliModelCatalog, Script}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._


case class ListInstances(
  modelDefinitionId: Option[Int], name: Option[String], dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[ListInstances])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {

    log.info(s"List Model Matrix instances. " +
      s"Definition id: ${modelDefinitionId.getOrElse("-")}. " +
      s"Name: ${name.getOrElse("-")}. " +
      s"Database: $dbName @ ${dbConfig.origin()}")

    blockOn(db.run(modelInstances.list(modelDefinitionId, name))).printASCIITable()
  }
}
