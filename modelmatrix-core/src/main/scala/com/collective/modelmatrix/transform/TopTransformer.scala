package com.collective.modelmatrix.transform

import com.collective.modelmatrix.CategorialColumn.AllOther
import com.collective.modelmatrix.transform.InputSchemaError.{ExtractColumnNotFound, UnsupportedTransformDataType}
import com.collective.modelmatrix.{CategorialColumn, ModelFeature}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scalaz.\/
import scalaz.syntax.either._

class TopTransformer(input: DataFrame) extends CategorialTransformer(input) {

  private val supportedDataTypes = Seq(ShortType, IntegerType, LongType, DoubleType, StringType)

  def validate: PartialFunction[ModelFeature, InputSchemaError \/ TypedModelFeature] = {
    case f@ModelFeature(_, _, _, e, Top(_, _)) if inputDataType(e).isEmpty =>
      ExtractColumnNotFound(e).left

    case f@ModelFeature(_, _, _, e, Top(_, _))
      if inputDataType(e).isDefined && supportedDataTypes.contains(inputDataType(e).get) =>
      TypedModelFeature(f, inputDataType(e).get).right

    case f@ModelFeature(_, _, _, e, t@Top(_, _)) =>
      UnsupportedTransformDataType(e, inputDataType(e).get, t).left
  }

  def transform(feature: TypedModelFeature): Seq[CategorialColumn] = {
    require(feature.feature.transform.isInstanceOf[Top], s"Illegal transform type: ${feature.feature.transform}")

    val ModelFeature(_, _, _, e, Top(percentage, allOther)) = feature.feature

    // Group and count by extract value
    val values: Seq[Value] = input.groupBy(e).count().collect().toSeq.map { row =>
      val value = row.get(0)
      val cnt = row.getLong(1)
      Value(value, cnt)
    }

    val topValues = values.sortBy(_.count)(implicitly[Ordering[Long]].reverse)

   // Get number of columns below percentage threshold
    val threshold = (percentage / 100) * topValues.map(_.count).sum
    val columnsBelowThreshold = topValues.map(_.count).scanLeft(0L)((cum, cnt) => cum + cnt).takeWhile(_ < threshold).size

    // Transform categorial values
    val valueColumns = topValues.take(columnsBelowThreshold).foldLeft(Scan()) {
      case (state@Scan(columnId, cumulativeCnt, columns), value) =>
        val column = valueColumn(feature.extractType)(columnId, cumulativeCnt, value)
        Scan(column.columnId, column.cumulativeCount, columns :+ column)
    }

    // Get all other columns if required
    val allOtherColumns = if (allOther) {
      val allOtherCnt = topValues.drop(columnsBelowThreshold).map(_.count).sum
      Seq(AllOther(valueColumns.columnId + 1, allOtherCnt, valueColumns.cumulativeCnt + allOtherCnt))
    } else Seq.empty

    // Add them together
    valueColumns.columns ++ allOtherColumns.filter(_.count > 0)
  }
}
