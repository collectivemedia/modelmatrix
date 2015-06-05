package com.collective.modelmatrix.cli.definition

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{CliModelCatalog, Script}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._

case class ViewSource(
  modelDefinitionId: Int, dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[ViewFeatures])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"View Model Matrix definition source: $modelDefinitionId. Database: $dbName @ ${dbConfig.origin()}")

    blockOn(db.run(modelDefinitions.findById(modelDefinitionId))) match {
      case Some(modelDefinition) =>
        Console.out.println(s"Model definition:")
        modelDefinition.printASCIITable()

        Console.out.println("Model Matrix definition source: \n\n")
        Console.out.println(modelDefinition.source)

      case None =>
        Console.out.println(s"Can't find model definition by id: $modelDefinitionId")
    }
  }
}
