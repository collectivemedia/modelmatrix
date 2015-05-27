package com.collective.modelmatrix.cli.definition

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{CliModelCatalog, Script}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._

case class ViewDefinition(
  modelDefinitionId: Int, dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[ViewDefinition])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"View Model Matrix definition: $modelDefinitionId. Database: $dbName @ ${dbConfig.origin().filename()}")

    blockOn(db.run(modelDefinitions.findById(modelDefinitionId))) match {
      case Some(modelDefinition) =>
        val features = blockOn(db.run(modelDefinitionFeatures.features(modelDefinitionId)))

        Console.out.println(s"Model definition:")
        modelDefinition.printASCIITable()

        Console.out.println(s"Model Matrix features: ${features.length}")
        features.printASCIITable()

      case None =>
        Console.out.println(s"Can't find model definition by id: $modelDefinitionId")
    }
  }
}
