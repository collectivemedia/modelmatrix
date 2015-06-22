package com.collective.modelmatrix.cli.definition

import com.collective.modelmatrix.ModelMatrix.PostgresModelMatrixCatalog
import com.collective.modelmatrix.cli.Script
import org.slf4j.LoggerFactory

case class ViewSource(
  modelDefinitionId: Int
) extends Script with PostgresModelMatrixCatalog {

  private val log = LoggerFactory.getLogger(classOf[ViewFeatures])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"View Model Matrix definition source: $modelDefinitionId")

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
