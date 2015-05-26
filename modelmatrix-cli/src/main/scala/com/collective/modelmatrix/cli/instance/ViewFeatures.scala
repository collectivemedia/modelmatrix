package com.collective.modelmatrix.cli.instance

import com.bethecoder.ascii_table.ASCIITable
import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{Script, CliModelCatalog}
import com.collective.modelmatrix.cli._
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scalaz._

/**
 * View Model Matrix instance by id
 */
case class ViewFeatures(
  modelInstanceId: Int, dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  def run(): Unit = {

    val modelHeader: Array[String] = Array(
      "Id", "Name", "Created By", "Created At", "Comment", "Features", "Columns"
    )
    
    val featuresHeader: Array[String] = Array(
      "Id", "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters", "Extract Type", "Columns"
    )

    val featuresNoData: Array[Array[String]] = Array(Array.fill(featuresHeader.length)("--"))

    val modelInstance = blockOn(db.run(modelInstances.findById(modelInstanceId)))
    modelInstance match {
      case Some(instance) =>

        val instanceCols = Array(
          instance.id.toString,
          instance.name.getOrElse("n/a"),
          instance.createdBy,
          timeFormatter.format(instance.createdAt),
          instance.comment.getOrElse("n/a"),
          instance.features.toString,
          instance.columns.toString
        )

        val features =
          blockOn(db.run(modelInstanceFeatures.features(modelInstanceId))).map { f =>
            Array(
              f.id.toString,
              f.feature.active.toString,
              f.feature.group,
              f.feature.feature,
              f.feature.extract,
              f.feature.transform.stringify,
              printParameters(f.feature.transform),
              f.extractType.toString
            )
          }

        Console.out.println(s"Model instance:")
        ASCIITable.getInstance().printTable(modelHeader, Array(instanceCols))

        Console.out.println(s"Model Matrix instance features: ${features.length}")
        ASCIITable.getInstance().printTable(featuresHeader, if (features.nonEmpty) features.toArray else featuresNoData)

      case None =>
        Console.out.println(s"Can't find model instance by id: $modelInstanceId")
    }
  }
}
