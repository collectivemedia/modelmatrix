package com.collective.modelmatrix.transform

import java.nio.ByteBuffer

import com.collective.modelmatrix.CategorialColumn.CategorialValue
import com.collective.modelmatrix.{CategorialColumn, ModelFeature}
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scodec.bits.ByteVector

import scala.util.{Success, Failure, Try}
import scalaz.{\/-, -\/, @@, \/}

case class TypedModelFeature(feature: ModelFeature, extractType: DataType)

sealed trait FeatureTransformationError {
  def errorMessage: String
}

object FeatureTransformationError {

  case class FeatureColumnNotFound(column: String) extends FeatureTransformationError {
    def errorMessage: String = s"Can't find feature column: $column"
  }

  case class UnsupportedTransformDataType(
    extract: String,
    dataType: DataType,
    transform: Transform
  ) extends FeatureTransformationError {
    def errorMessage: String = s"Unsupported feature data type: ${dataType.typeName} for transformation: $transform"
  }

}

abstract class Transformer(features: DataFrame @@ Transformer.Features) {

  def validate: PartialFunction[ModelFeature, FeatureTransformationError \/ TypedModelFeature]

  protected def featureDataType(feature: String): Option[DataType] = {
    scalaz.Tag.unwrap(features).schema.find(_.name == feature).map(_.dataType)
  }

}

object Transformer {

  case class FeatureExtractionError(feature: ModelFeature, error: Throwable)

  /**
   * Marker for DataFrame with applied extract expressions
   */
  trait Features

  /**
   * Marker for DataFrame with applied extract expressions + Id columns
   */
  trait FeaturesWithId

  type FeaturesDataFrame = DataFrame  @@ Features
  type FeaturesWithIdDataFrame = DataFrame @@ FeaturesWithId

  private def tryExtract(df: DataFrame, features: Seq[ModelFeature]): (Seq[FeatureExtractionError], Seq[String]) = {
    val tryExtract = features map { case feature =>
      val expr = s"${feature.extract} as ${feature.feature}"
      Try(df.selectExpr(expr).head()) match {
        case Failure(err) => \/.left(FeatureExtractionError(feature, err))
        case Success(_) => \/.right(expr)
      }
    }

    val errors = tryExtract.collect { case -\/(err) => err }
    val expressions = tryExtract.collect { case \/-(expr) => expr }

    (errors, expressions)
  }

  def selectFeatures(
    df: DataFrame,
    features: Seq[ModelFeature]
  ): Seq[FeatureExtractionError] \/ FeaturesDataFrame= {
    val (errors, expressions) = tryExtract(df, features)
    if (errors.nonEmpty) {
      \/.left(errors)
    } else {
      val extracted = df.selectExpr(expressions:_*)
      \/.right(scalaz.Tag[DataFrame, Features](extracted))
    }
  }

  def selectFeaturesWithId(
    df: DataFrame,
    idColumn: String,
    features: Seq[ModelFeature]
  ): Seq[FeatureExtractionError] \/ FeaturesWithIdDataFrame = {
    val (errors, expressions) = tryExtract(df, features)
    if (errors.nonEmpty) {
      \/.left(errors)
    } else {
      val extracted = df.selectExpr(idColumn +: expressions:_*)
      \/.right(scalaz.Tag[DataFrame, FeaturesWithId](extracted))
    }
  }

  /**
   * Actually none of the columns are removed, just re-tagging DataFrame
   */
  def removeIdColumn(df: DataFrame @@ FeaturesWithId): DataFrame @@ Features = {
    scalaz.Tag[DataFrame, Features](scalaz.Tag.unwrap(df))
  }
  
}

abstract class CategorialTransformer(features: DataFrame @@ Transformer.Features) extends Transformer(features) {

  def transform(feature: TypedModelFeature): Seq[CategorialColumn]

  protected case class Value(value: Any, count: Long)

  protected case class Scan(columnId: Int = 0, cumulativeCnt: Long = 0, columns: Seq[CategorialColumn] = Seq.empty)

  protected def valueColumn(extractType: DataType)(
    previousColumnId: Int,
    previousCumCnt: Long,
    value: Value
  ): CategorialValue = value match {
    case Value(s: Short, cnt) if extractType == ShortType =>
      val bb = ByteBuffer.allocate(2).putShort(s)
      CategorialValue(previousColumnId + 1, s.toString, ByteVector(bb), cnt, previousCumCnt + cnt)

    case Value(i: Int, cnt) if extractType == IntegerType =>
      val bb = ByteBuffer.allocate(4).putInt(i)
      CategorialValue(previousColumnId + 1, i.toString, ByteVector(bb), cnt, previousCumCnt + cnt)

    case Value(l: Long, cnt) if extractType == LongType =>
      val bb = ByteBuffer.allocate(8).putLong(l)
      CategorialValue(previousColumnId + 1, l.toString, ByteVector(bb), cnt, previousCumCnt + cnt)

    case Value(d: Double, cnt) if extractType == DoubleType =>
      val bb = ByteBuffer.allocate(8).putDouble(d)
      CategorialValue(previousColumnId + 1, d.toString, ByteVector(bb), cnt, previousCumCnt + cnt)

    case Value(s: String, cnt) if extractType == StringType =>
      CategorialValue(previousColumnId + 1, s.toString, ByteVector(s.getBytes), cnt, previousCumCnt + cnt)

    case Value(v, cnt) =>
      sys.error(s"Unsupported value: $v. Class of: ${v.getClass}. Extract type: $extractType")
  }


}
