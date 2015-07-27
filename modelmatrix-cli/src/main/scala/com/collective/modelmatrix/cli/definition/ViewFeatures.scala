package com.collective.modelmatrix.cli.definition

import com.collective.modelmatrix.ModelMatrix.DbModelMatrixCatalog
import com.collective.modelmatrix.cli.Script
import org.slf4j.LoggerFactory

case class ViewFeatures(
  modelDefinitionId: Int
) extends Script with DbModelMatrixCatalog {

  private val log = LoggerFactory.getLogger(classOf[ViewFeatures])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"View Model Matrix definition features: $modelDefinitionId")

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
