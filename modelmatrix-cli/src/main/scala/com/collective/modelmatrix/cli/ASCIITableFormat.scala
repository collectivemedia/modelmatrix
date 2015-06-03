package com.collective.modelmatrix.cli

import com.bethecoder.ascii_table.{ASCIITable, ASCIITableHeader}
import com.collective.modelmatrix.CategorialColumn.{AllOther, CategorialValue}
import com.collective.modelmatrix.{BinColumn, FeatureSchemaError, CategorialColumn, ModelFeature}
import com.collective.modelmatrix.catalog._
import com.collective.modelmatrix.transform._
import org.apache.spark.sql.catalyst.SqlParser

import scalaz.{\/-, -\/, \/}


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

  private val sqlParser = new SqlParser()

  private def formatExtractExpr(s: String): String = {
    sqlParser.parseExpression(s).toString()
  }

  implicit class StringFormattingOps(val s: String) extends AnyVal {
    def bounded(n: Int): String = if (s.length > n) s"${s.take(n-4)} ..." else s
  }

  private def printParameters(t: Transform): String = t match {
    case Identity => ""
    case Top(p, ao) => s"cover = $p; allOther = $ao"
    case Index(p, ao) => s"support = $p; allOther = $ao"
    case Bins(nbins, p, pct) => s"nbins = $nbins; minpts = $p; minpct = $pct"
  }

  implicit val modelFeatureFormat: ASCIITableFormat[ModelFeature] =
    ASCIITableFormat[ModelFeature](
      "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters"
    ) { feature =>
      Array(
        feature.active.toString,
        feature.group,
        feature.feature,
        formatExtractExpr(feature.extract),
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
        definition.name.getOrElse(""),
        definition.createdBy,
        timeFormatter.format(definition.createdAt),
        definition.comment.getOrElse(""),
        definition.features.toString
      )
    }

  implicit val modelDefinitionFeatureFormat: ASCIITableFormat[ModelDefinitionFeature] =
    ASCIITableFormat[ModelDefinitionFeature](
      "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters"
    ) { definitionFeature =>
      Array(
        definitionFeature.feature.active.toString,
        definitionFeature.feature.group,
        definitionFeature.feature.feature,
        formatExtractExpr(definitionFeature.feature.extract),
        definitionFeature.feature.transform.stringify,
        printParameters(definitionFeature.feature.transform)
      )
    }

  implicit val modelInstanceFormat: ASCIITableFormat[ModelInstance] =
    ASCIITableFormat[ModelInstance](
      "Id", "Definition Id", "Name", "Created By", "Created At", "Comment", "Features", "Columns"
    ) { instance =>
      Array(
        instance.id.toString,
        instance.modelDefinitionId.toString,
        instance.name.getOrElse(""),
        instance.createdBy,
        timeFormatter.format(instance.createdAt),
        instance.comment.getOrElse(""),
        instance.features.toString,
        instance.columns.toString
      )
    }

  implicit val modelInstanceFeatureFormat: ASCIITableFormat[ModelInstanceFeature] =
    ASCIITableFormat[ModelInstanceFeature](
      "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters", "Extract Type", "Columns"
    ) { f =>
      Array(
        f.feature.active.toString,
        f.feature.group,
        f.feature.feature,
        formatExtractExpr(f.feature.extract),
        f.feature.transform.stringify,
        printParameters(f.feature.transform),
        f.extractType.toString,
        f match {
          case f: ModelInstanceIdentityFeature => "1"
          case f: ModelInstanceTopFeature => f.columns.size.toString
          case f: ModelInstanceIndexFeature => f.columns.size.toString
          case f: ModelInstanceBinsFeature => f.columns.size.toString
        }
      )
    }

  private def formatBinColumn(feature: ModelFeature)(column: BinColumn): Array[String] = {
      Array(
        column.columnId.toString,
        feature.feature,
        feature.transform.stringify,
        column match {
          case lower: BinColumn.LowerBin => s"(-Inf, ${lower.high})"
          case upper: BinColumn.UpperBin => s"[${upper.low}, +Inf)"
          case value: BinColumn.BinValue => s"[${value.low}, ${value.high})}"
        },
        column.count.toString,
        column.sampleSize.toString
      )
  }

  private def formatCategorialColumn(feature: ModelFeature)(column: CategorialColumn): Array[String] = column match {
    case value: CategorialValue =>
      Array(
        value.columnId.toString,
        feature.feature,
        feature.transform.stringify,
        value.sourceName.bounded(50),
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

  type InstanceFeature = (ModelInstanceFeature, Option[CategorialColumn \/ BinColumn])

  implicit val modelInstanceFeatureColumnsFormat: ASCIITableFormat[InstanceFeature] =
    ASCIITableFormat[InstanceFeature](
      "Columns Id", "Feature", "Transform", "Name".dataLeftAligned, "Count", "Cumulative Count / Sample Size"
    ) {
      case (f@ModelInstanceIdentityFeature(_, _, _, _, columnId), None) =>
        Array(
          columnId.toString,
          f.feature.feature,
          f.feature.transform.stringify,
          formatExtractExpr(f.feature.extract),
          "", ""
        )
      case (f@ModelInstanceTopFeature(_, _, _, _, _), Some(-\/(col))) => formatCategorialColumn(f.feature)(col)
      case (f@ModelInstanceIndexFeature(_, _, _, _, _), Some(-\/(col))) => formatCategorialColumn(f.feature)(col)
      case (f@ModelInstanceBinsFeature(_, _, _, _, _), Some(\/-(col))) => formatBinColumn(f.feature)(col)
      case (f, col) => sys.error(s"Unsupported feature: $f column: $col")
    }

  implicit val inputSchemaErrorFormat: ASCIITableFormat[(ModelDefinitionFeature, TransformSchemaError)] =
    ASCIITableFormat[(ModelDefinitionFeature, TransformSchemaError)](
      "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters", "Error".dataLeftAligned
    ) { inputSchemaError =>
      val (ModelDefinitionFeature(id, _, feature), error) = inputSchemaError
      Array(
        feature.active.toString,
        feature.group,
        feature.feature,
        formatExtractExpr(feature.extract),
        feature.transform.stringify,
        printParameters(feature.transform),
        error.errorMessage
      )
    }

  implicit val inputSchemaTypedFeatureFormat: ASCIITableFormat[(ModelDefinitionFeature, TypedModelFeature)] =
    ASCIITableFormat[(ModelDefinitionFeature, TypedModelFeature)](
      "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters", "Extract Type"
    ) { inputSchemaTyped =>
      val (ModelDefinitionFeature(id, _, feature), typed) = inputSchemaTyped
      Array(
        feature.active.toString,
        feature.group,
        feature.feature,
        formatExtractExpr(feature.extract),
        feature.transform.stringify,
        printParameters(feature.transform),
        typed.extractType.toString
      )
    }

  implicit val featureSchemaErrorFormat: ASCIITableFormat[FeatureSchemaError] =
    ASCIITableFormat[FeatureSchemaError](
      "Feature", "Error".dataLeftAligned
    ) { error =>
      Array(
        error.feature,
        error.errorMessage
      )
    }


}
