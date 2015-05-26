package com.collective.modelmatrix.cli.instance

import com.bethecoder.ascii_table.{ASCIITableHeader, ASCIITable}
import com.collective.modelmatrix.CategorialColumn.{AllOther, CategorialValue}
import com.collective.modelmatrix.catalog.{ModelInstanceIndexFeature, ModelInstanceTopFeature, ModelInstanceIdentityFeature, ModelMatrixCatalog}
import com.collective.modelmatrix.cli._
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scalaz._

/**
 * View Model Matrix instance by id
 */
case class ViewColumns(
  modelInstanceId: Int, dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  def run(): Unit = {

    val modelHeader: Array[ASCIITableHeader] = Array(
      "Id", "Name", "Created By", "Created At", "Comment", "Features", "Columns"
    )

    val columnsHeader: Array[ASCIITableHeader] = Array(
      "Columns Id", "Feature", "Transform", "Source Name", "Count", "Cumulative Count"
    )

    val columnsNoData: Array[Array[String]] = Array(Array.fill(columnsHeader.length)("--"))

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

        val features: Seq[Array[String]] =
          blockOn(db.run(modelInstanceFeatures.features(modelInstanceId))).flatMap {
            case f@ModelInstanceIdentityFeature(_, _, _, _, columnId) =>
              Seq(Array(
                columnId.toString,
                f.feature.feature,
                f.feature.transform.stringify,
                f.feature.extract,
                "n/a",
                "n/a"
              ))

            case f@ModelInstanceTopFeature(_, _, _, _, columns) => columns.map {
              case value: CategorialValue =>
                Array(
                  value.columnId.toString,
                  f.feature.feature,
                  f.feature.transform.stringify,
                  value.sourceName,
                  value.count.toString,
                  value.cumulativeCount.toString
                )

              case value: AllOther =>
                Array(
                  value.columnId.toString,
                  f.feature.feature,
                  f.feature.transform.stringify,
                  "all other",
                  value.count.toString,
                  value.cumulativeCount.toString
                )
            }

            case f@ModelInstanceIndexFeature(_, _, _, _, columns) => columns.map {
              case value: CategorialValue =>
                Array(
                  value.columnId.toString,
                  f.feature.feature,
                  f.feature.transform.stringify,
                  value.sourceName,
                  value.count.toString,
                  value.cumulativeCount.toString
                )

              case value: AllOther =>
                Array(
                  value.columnId.toString,
                  f.feature.feature,
                  f.feature.transform.stringify,
                  "all other",
                  value.count.toString,
                  value.cumulativeCount.toString
                )
            }

          }

        Console.out.println(s"Model instance:")
        ASCIITable.getInstance().printTable(modelHeader, Array(instanceCols))

        Console.out.println(s"Model Matrix instance columns: ${features.length}")
        ASCIITable.getInstance().printTable(columnsHeader, if (features.nonEmpty) features.toArray else columnsNoData)

      case None =>
        Console.out.println(s"Can't find model instance by id: $modelInstanceId")
    }
  }
}
