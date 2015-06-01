package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.catalog.{ModelInstance, ModelMatrixCatalog}
import com.collective.modelmatrix.cli.{CliModelCatalog, Script}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._


case class ListInstances(
  modelDefinitionId: Option[Int], dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[ListInstances])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"View all Model Matrix instances. Database: $dbName @ ${dbConfig.origin()}")
    val definitionFilter = if (modelDefinitionId.isDefined) {
      (_: ModelInstance).modelDefinitionId == modelDefinitionId.get
    } else (_: ModelInstance) => true
    blockOn(db.run(modelInstances.all)).filter(definitionFilter).printASCIITable()
  }
}
