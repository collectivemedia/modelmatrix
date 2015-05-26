package com.collective.modelmatrix.cli.definition

import com.bethecoder.ascii_table.ASCIITable
import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{Script, CliModelCatalog}
import com.collective.modelmatrix.cli._
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scalaz._

/**
 * View Model Matrix definition by id
 */
case class View(
  modelDefinitionId: Int, dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  def run(): Unit = {
    
    val featuresHeader: Array[String] = Array(
      "Id", "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters"
    )

    val featuresNoData: Array[Array[String]] = Array(Array.fill(featuresHeader.length)("--"))

    val modelHeader: Array[String] = Array(
      "Id", "Name", "Created By", "Created At", "Comment", "Features"
    )

    val modelDefinition = blockOn(db.run(modelDefinitions.findById(modelDefinitionId)))
    modelDefinition match {
      case Some(definition) =>

        val definitionCols = Array(
          definition.id.toString,
          definition.name.getOrElse("n/a"),
          definition.createdBy,
          timeFormatter.format(definition.createdAt),
          definition.comment.getOrElse("n/a"),
          definition.features.toString
        )

        val features =
          blockOn(db.run(modelDefinitionFeatures.features(modelDefinitionId))).map { f =>
            Array(
              f.id.toString,
              f.feature.active.toString,
              f.feature.group,
              f.feature.feature,
              f.feature.extract,
              f.feature.transform.stringify,
              printParameters(f.feature.transform)
            )
          }

        Console.out.println(s"Model definition:")
        ASCIITable.getInstance().printTable(modelHeader, Array(definitionCols))

        Console.out.println(s"Model Matrix features: ${features.length}")
        ASCIITable.getInstance().printTable(featuresHeader, if (features.nonEmpty) features.toArray else featuresNoData)

      case None =>
        Console.out.println(s"Can't find model definition by id: $modelDefinitionId")        
    }
  }
}
