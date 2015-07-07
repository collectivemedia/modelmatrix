package com.collective.modelmatrix.transform

import com.collective.modelmatrix.CategorialColumn.AllOther
import com.collective.modelmatrix.{CategorialColumn, ModelFeature}
import com.collective.modelmatrix.transform.FeatureTransformationError.{UnsupportedTransformDataType, FeatureColumnNotFound}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scalaz.{@@, \/}
import scalaz.syntax.either._

class IndexTransformer(input: DataFrame @@ Transformer.Features) extends CategorialTransformer(input) {

  private val log = LoggerFactory.getLogger(classOf[IndexTransformer])

  private val supportedDataTypes = Seq(ShortType, IntegerType, LongType, DoubleType, StringType)

  def validate: PartialFunction[ModelFeature, FeatureTransformationError \/ TypedModelFeature] = {
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

    val df = scalaz.Tag.unwrap(input)

    import org.apache.spark.sql.functions._

    // Group and count by extract value
    val grouped: DataFrame = df.filter(df(f).isNotNull).groupBy(f).count()

    // Get support threshold
    val totalCount = grouped.sumOf("count")
    val threshold = (support / 100) * totalCount

    // Collect only support values
    val supportValues: Seq[Value] = grouped.filter(col("count") > threshold).collect().toSeq.map { row =>
      val value = row.get(0)
      val cnt = row.getLong(1)
      Value(value, cnt)
    }
    val topSupportValues = supportValues.sortBy(_.count)(implicitly[Ordering[Long]].reverse)

    log.debug(s"Collected support values: ${supportValues.size}")

    // Transform categorial values
    val valueColumns = topSupportValues.foldLeft(Scan()) {
      case (state@Scan(columnId, cumulativeCnt, columns), value) =>
        val column = valueColumn(feature.extractType)(columnId, cumulativeCnt, value)
        Scan(column.columnId, column.cumulativeCount, columns :+ column)
    }

    // Get all other column if required
    val allOtherColumns = if (allOther && support < 100.0) {
      // Count for values that are not in support set
      val allOtherCnt = grouped.filter(col("count") <= threshold).sumOf("count")
      Seq(AllOther(valueColumns.columnId + 1, allOtherCnt, valueColumns.cumulativeCnt + allOtherCnt))
    } else Seq.empty

    // Add them together
    valueColumns.columns ++ allOtherColumns.filter(_.count > 0)
  }
}
