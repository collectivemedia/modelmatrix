package com.collective.modelmatrix.transform

import com.collective.modelmatrix.CategorialColumn.AllOther
import com.collective.modelmatrix.{CategorialColumn, ModelFeature}
import com.collective.modelmatrix.transform.TransformSchemaError.{UnsupportedTransformDataType, FeatureColumnNotFound}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scalaz.{@@, \/}
import scalaz.syntax.either._

class IndexTransformer(input: DataFrame @@ Transformer.Features) extends CategorialTransformer(input) {

  private val log = LoggerFactory.getLogger(classOf[IndexTransformer])

  private val supportedDataTypes = Seq(ShortType, IntegerType, LongType, DoubleType, StringType)

  def validate: PartialFunction[ModelFeature, TransformSchemaError \/ TypedModelFeature] = {
    case f@ModelFeature(_, _, _, _, Index(_, _)) if featureDataType(f.feature).isEmpty =>
      FeatureColumnNotFound(f.feature).left

    case f@ModelFeature(_, _, _, _, Index(_, _))
      if featureDataType(f.feature).isDefined && supportedDataTypes.contains(featureDataType(f.feature).get) =>
      TypedModelFeature(f, featureDataType(f.feature).get).right

    case f@ModelFeature(_, _, _, _, t@Index(_, _)) =>
      UnsupportedTransformDataType(f.feature, featureDataType(f.feature).get, t).left
  }

  def transform(feature: TypedModelFeature): Seq[CategorialColumn] = {
    require(feature.feature.transform.isInstanceOf[Index], s"Illegal transform type: ${feature.feature.transform}")

    val ModelFeature(_, _, f, _, Index(support, allOther)) = feature.feature

    log.info(s"Calculate index transformation for feature: ${feature.feature.feature}. " +
      s"Support: $support. " +
      s"All other: $allOther. " +
      s"Extract type: ${feature.extractType}")

    // Group and count by extract value
    val values: Seq[Value] = scalaz.Tag.unwrap(input).groupBy(f).count().collect().toSeq.map { row =>
      val value = row.get(0)
      val cnt = row.getLong(1)
      Value(value, cnt)
    }

    log.debug(s"Collected support values: ${values.size}")

    val topValues = values.sortBy(_.count)(implicitly[Ordering[Long]].reverse)

    // Get support threshold
    val threshold = (support / 100) * topValues.map(_.count).sum

    // Transform categorial values
    val valueColumns = topValues.filter(_.count > threshold).foldLeft(Scan()) {
      case (state@Scan(columnId, cumulativeCnt, columns), value) =>
        val column = valueColumn(feature.extractType)(columnId, cumulativeCnt, value)
        Scan(column.columnId, column.cumulativeCount, columns :+ column)
    }

    // Get all other columns if required
    val allOtherColumns = if (allOther) {
      val allOtherCnt = topValues.filter(_.count <= threshold).map(_.count).sum
      Seq(AllOther(valueColumns.columnId + 1, allOtherCnt, valueColumns.cumulativeCnt + allOtherCnt))
    } else Seq.empty

    // Add them together
    valueColumns.columns ++ allOtherColumns.filter(_.count > 0)
  }
}
