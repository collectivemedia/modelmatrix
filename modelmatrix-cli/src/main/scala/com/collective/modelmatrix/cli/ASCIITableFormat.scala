package com.collective.modelmatrix.cli

import com.bethecoder.ascii_table.{ASCIITable, ASCIITableHeader}
import com.collective.modelmatrix.CategorialColumn.{AllOther, CategorialValue}
import com.collective.modelmatrix.{CategorialColumn, ModelFeature}
import com.collective.modelmatrix.catalog._
import com.collective.modelmatrix.transform._


abstract class ASCIITableFormat[T](val header: Array[ASCIITableHeader]) {
  def row(obj: T): Array[String]

  def noData: Array[Array[String]] = Array(Array.empty)
}

object ASCIITableFormat {

  def apply[T](header: ASCIITableHeader*)(f: T => Array[String]): ASCIITableFormat[T] = {
    new ASCIITableFormat[T](header.toArray) {
      def row(obj: T): Array[String] = f(obj)
    }
  }

  implicit class ASCIIOps[T: ASCIITableFormat](val obj: T) {
    def toASCIITableHeader: Array[ASCIITableHeader] = implicitly[ASCIITableFormat[T]].header

    def toASCIITableRow: Array[String] = implicitly[ASCIITableFormat[T]].row(obj)

    def printASCIITable(): Unit = {
      val format = implicitly[ASCIITableFormat[T]]
      val rows = Array(obj.toASCIITableRow)
      ASCIITable.getInstance().printTable(format.header, rows)
    }
  }
  
  implicit class ASCIISeqOps[T: ASCIITableFormat](val values: Seq[T]) {
    def printASCIITable(): Unit = {
      val format = implicitly[ASCIITableFormat[T]]
      val rows = if (values.nonEmpty) values.map(_.toASCIITableRow).toArray else format.noData
      ASCIITable.getInstance().printTable(format.header, rows)
    }
  }  
}

object ASCIITableFormats {

  private def printParameters(t: Transform): String = t match {
    case Identity => ""
    case Top(p, ao) => s"percentage = $p; allOther = $ao"
    case Index(p, ao) => s"percentage = $p; allOther = $ao"
  }

  implicit val modelFeatureFormat: ASCIITableFormat[ModelFeature] =
    ASCIITableFormat[ModelFeature](
      "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters"
    ) { feature =>
      Array(
        feature.active.toString,
        feature.group,
        feature.feature,
        feature.extract,
        feature.transform.stringify,
        printParameters(feature.transform)
      )
    }

  implicit val modelDefinitionFormat: ASCIITableFormat[ModelDefinition] =
    ASCIITableFormat[ModelDefinition](
      "Id", "Name", "Created By", "Created At", "Comment", "Features"
    ) { definition =>
      Array(
        definition.id.toString,
        definition.name.getOrElse("n/a"),
        definition.createdBy,
        timeFormatter.format(definition.createdAt),
        definition.comment.getOrElse("n/a"),
        definition.features.toString
      )
    }

  implicit val modelDefinitionFeatureFormat: ASCIITableFormat[ModelDefinitionFeature] =
    ASCIITableFormat[ModelDefinitionFeature](
      "Id", "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters"
    ) { definitionFeature =>
      Array(
        definitionFeature.id.toString,
        definitionFeature.feature.active.toString,
        definitionFeature.feature.group,
        definitionFeature.feature.feature,
        definitionFeature.feature.extract,
        definitionFeature.feature.transform.stringify,
        printParameters(definitionFeature.feature.transform)
      )
    }

  implicit val modelInstanceFormat: ASCIITableFormat[ModelInstance] =
    ASCIITableFormat[ModelInstance](
      "Id", "Name", "Created By", "Created At", "Comment", "Features", "Columns"
    ) { instance =>
      Array(
        instance.id.toString,
        instance.name.getOrElse("n/a"),
        instance.createdBy,
        timeFormatter.format(instance.createdAt),
        instance.comment.getOrElse("n/a"),
        instance.features.toString,
        instance.columns.toString
      )
    }

  implicit val modelInstanceFeatureFormat: ASCIITableFormat[ModelInstanceFeature] =
    ASCIITableFormat[ModelInstanceFeature](
      "Id", "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters", "Extract Type", "Columns"
    ) { f =>
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

  private def formatCategorialColumn(feature: ModelFeature)(column: CategorialColumn): Array[String] = column match {
    case value: CategorialValue =>
      Array(
        value.columnId.toString,
        feature.feature,
        feature.transform.stringify,
        value.sourceName,
        value.count.toString,
        value.cumulativeCount.toString
      )

    case value: AllOther =>
      Array(
        value.columnId.toString,
        feature.feature,
        feature.transform.stringify,
        "all other",
        value.count.toString,
        value.cumulativeCount.toString
      )
  }

  implicit val modelInstanceFeatureColumnsFormat: ASCIITableFormat[(ModelInstanceFeature, Option[CategorialColumn])] =
    ASCIITableFormat[(ModelInstanceFeature, Option[CategorialColumn])](
      "Columns Id", "Feature", "Transform", "Source Name", "Count", "Cumulative Count"
    ) {
      case (f@ModelInstanceIdentityFeature(_, _, _, _, columnId), None) =>
        Array(
          columnId.toString,
          f.feature.feature,
          f.feature.transform.stringify,
          f.feature.extract,
          "n/a", "n/a"
        )
      case (f@ModelInstanceTopFeature(_, _, _, _, _), Some(col)) => formatCategorialColumn(f.feature)(col)
      case (f@ModelInstanceIndexFeature(_, _, _, _, _), Some(col)) => formatCategorialColumn(f.feature)(col)
      case (f, col) => sys.error(s"Unsupported feature: $f column: $col")
    }

  implicit val inputSchemaErrorFormat: ASCIITableFormat[(ModelDefinitionFeature, InputSchemaError)] =
    ASCIITableFormat[(ModelDefinitionFeature, InputSchemaError)](
      "Id", "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters", "Error"
    ) { inputSchemaError =>
      val (ModelDefinitionFeature(id, _, feature), error) = inputSchemaError
      Array(
        id.toString,
        feature.active.toString,
        feature.group,
        feature.feature,
        feature.extract,
        feature.transform.stringify,
        printParameters(feature.transform),
        error.errorMessage
      )
    }

  implicit val inputSchemaTypedFeatureFormat: ASCIITableFormat[(ModelDefinitionFeature, TypedModelFeature)] =
    ASCIITableFormat[(ModelDefinitionFeature, TypedModelFeature)](
      "Id", "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters", "Extract Type"
    ) { inputSchemaTyped =>
      val (ModelDefinitionFeature(id, _, feature), typed) = inputSchemaTyped
      Array(
        id.toString,
        feature.active.toString,
        feature.group,
        feature.feature,
        feature.extract,
        feature.transform.stringify,
        printParameters(feature.transform),
        typed.extractType.toString
      )
    }


}
