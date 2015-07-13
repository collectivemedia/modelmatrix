package com.collective.modelmatrix

import java.nio.ByteBuffer

import com.collective.modelmatrix.CategoricalColumn.{AllOther, CategoricalValue}
import com.collective.modelmatrix.catalog._
import com.collective.modelmatrix.transform.Transformer.{Features, LabeledFeatures}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.slf4j.LoggerFactory

import scalaz._
import scalaz.syntax.either._

sealed trait FeaturizationError {
  def feature: String
  def errorMessage: String
}

object FeaturizationError {

  case class FeatureColumnNotFound(feature: String) extends FeaturizationError {
    def errorMessage: String = s"Can't find feature column: $feature"
  }

  case class FeatureColumnTypeDoesNotMatch(
    feature: String,
    expected: DataType,
    found: DataType
  ) extends FeaturizationError {
    def errorMessage: String = s"Feature column: $feature type: $found doesn't match expected: $expected"
  }
}

class Featurization(features: Seq[ModelInstanceFeature]) extends Serializable {

  private val log = LoggerFactory.getLogger(classOf[Featurization])

  import FeaturizationError._

  // Check that all input features belong to the same model instance
  private val instances = features.map(_.modelInstanceId).toSet
  require(instances.size == 1, s"Features belong to different model instances: $instances")

  // Maximum columns id in instance features
  private val totalNumberOfColumns = features.flatMap {
    case ModelInstanceIdentityFeature(_, _, _, _, columnId) => Seq(columnId)
    case ModelInstanceTopFeature(_, _, _, _, cols) => cols.map(_.columnId)
    case ModelInstanceIndexFeature(_, _, _, _, cols) => cols.map(_.columnId)
    case ModelInstanceBinsFeature(_, _, _, _, cols) => cols.map(_.columnId)
  }.max

  type FeatureColumnId = (ModelFeature, Int)

  def validateLabeled[L](input: DataFrame @@ LabeledFeatures[L]): Seq[FeaturizationError \/ Column] = {
    validate(Tag.unwrap(input))
  }

  def validate[L](input: DataFrame @@ Features): Seq[FeaturizationError \/ Column] = {
    validate(Tag.unwrap(input))
  }

  private def validate[L](df: DataFrame): Seq[FeaturizationError \/ Column] = {

    def featureDataType(feature: String): Option[DataType] = {
      df.schema.find(_.name == feature).map(_.dataType)
    }

    features.map {
      // Valid features
      case ModelInstanceIdentityFeature(_, _, feature, extractType, _)
        if featureDataType(feature.feature).exists(_ == extractType) => new Column(feature.feature).right
      case ModelInstanceTopFeature(_, _, feature, extractType, _)
        if featureDataType(feature.feature).exists(_ == extractType) => new Column(feature.feature).right
      case ModelInstanceIndexFeature(_, _, feature, extractType, _)
        if featureDataType(feature.feature).exists(_ == extractType) => new Column(feature.feature).right
      case ModelInstanceBinsFeature(_, _, feature, extractType, _)
        if featureDataType(feature.feature).exists(_ == extractType) => new Column(feature.feature).right

      // Validation errors
      case f: ModelInstanceFeature if featureDataType(f.feature.feature).isEmpty =>
        FeatureColumnNotFound(f.feature.feature).left
      case f: ModelInstanceFeature =>
        FeatureColumnTypeDoesNotMatch(
          f.feature.feature,
          featureDataType(f.feature.feature).get,
          f.extractType
        ).left
    }
  }


  private def readColumns(row: Row, rebaseIdx: Int => Int = identity): Seq[(Int, Double)] = {
    val featuresColumnIdx: Map[ModelFeature, Int] =
      features.zipWithIndex.map { case (f, idx) => (f.feature, rebaseIdx(idx)) }.toMap

    features.flatMap {
      case ModelInstanceIdentityFeature(_, _, f, tpe, columnId) =>
        identityColumn(row)(f, featuresColumnIdx(f), tpe, columnId).toSeq
      case ModelInstanceTopFeature(_, _, f, tpe, cols) =>
        categoricalColumn(row)(f, featuresColumnIdx(f), tpe, cols).toSeq
      case ModelInstanceIndexFeature(_, _, f, tpe, cols) =>
        categoricalColumn(row)(f, featuresColumnIdx(f), tpe, cols).toSeq
      case ModelInstanceBinsFeature(_, _, f, tpe, cols) =>
        binColumn(row)(f, featuresColumnIdx(f), tpe, cols).toSeq
    }
  }

  private def validateColumns[L](df: DataFrame): Seq[Column] = {
    val validation = validate(df)
    val errors = validation.collect { case -\/(error) => error }
    require(errors.isEmpty,
      s"\nFound ${errors.size} input data errors: \n ${formatFeatureErrors(errors)}")
    validation.collect { case \/-(column) => column }
  }

  def featurize[L](input: DataFrame @@ Features): RDD[Vector] = {
    log.info(s"Extract features from input DataFrame. Total number of columns: $totalNumberOfColumns")

    val df = scalaz.Tag.unwrap(input)
    val featureColumns = validateColumns(df)
    df.select(featureColumns: _*).map { row =>
      // Read feature columns
      val featureValues = readColumns(row)
      // Column values 1-based and Vector values are 0-based
      val vectorValues = featureValues.map { case (idx, v) => (idx - 1, v) }
      Vectors.sparse(totalNumberOfColumns, vectorValues)
    }
  }

  def featurize[L](input: DataFrame @@ LabeledFeatures[L], labeling: Labeling[L]): RDD[(L, Vector)] = {
    log.info(s"Extract features from input DataFrame. Total number of columns: $totalNumberOfColumns")

    val df = scalaz.Tag.unwrap(input)

    val labelingColumns = labeling.expressions.map(col)
    val labelingIdx = labelingColumns.zipWithIndex.map(_._2)
    val featureColumns = validateColumns(df)

    df.select(labelingColumns ++ featureColumns: _*).map { row =>
      // Read labeling columns
      val labelingValues = labelingIdx.map(row.get)
      val label = labeling.label(labelingValues)

      // Read feature columns
      val featureValues = readColumns(row, _ + labelingIdx.size)
      // Column values 1-based and Vector values are 0-based
      val vectorValues = featureValues.map { case (idx, v) => (idx - 1, v) }

      label -> Vectors.sparse(totalNumberOfColumns, vectorValues)
    }
  }

  private def identityColumn(row: Row)(
    feature: ModelFeature,
    idx: Int,
    extractType: DataType,
    columnId: Int
  ): Option[(Int, Double)] = {

    // If input value is null just skip it
    if (row.isNullAt(idx)) return None

    val doubleValue = extractType match {
      case ShortType => row.getShort(idx).toDouble
      case IntegerType => row.getInt(idx).toDouble
      case LongType => row.getLong(idx).toDouble
      case DoubleType => row.getDouble(idx)
      case tpe => sys.error(s"Unsupported identity extract type: $tpe. Feature: ${feature.feature}")
    }

    Some((columnId, doubleValue))
  }

  private def categoricalColumn(row: Row)(
    feature: ModelFeature,
    idx: Int,
    extractType: DataType,
    columns: Seq[CategoricalColumn]
  ): Option[(Int, Double)] = {

    // If input value is null just skip it
    if (row.isNullAt(idx)) return None

    // Get byte representation of extracted feature
    val byteVector = extractType match {
      case ShortType => ModelMatrixEncoding.encode(row.getShort(idx))
      case IntegerType => ModelMatrixEncoding.encode(row.getInt(idx))
      case LongType => ModelMatrixEncoding.encode(row.getLong(idx))
      case DoubleType => ModelMatrixEncoding.encode(row.getDouble(idx))
      case StringType => ModelMatrixEncoding.encode(row.getString(idx))
      case tpe => sys.error(s"Unsupported categorical extract type: $tpe. Feature: ${feature.feature}. Columns: ${columns.size}")
    }

    // Take first matching categorical value or fallback to 'all other' if exists
    val categoricalColumn = columns.collect { case v: CategoricalValue if v.sourceValue == byteVector => v.columnId }
    val allOther = columns.collect { case AllOther(columnId, _, _) => columnId }

    (categoricalColumn ++ allOther).headOption.map((_, 1.0D))
  }

  private def binColumn(row: Row)(
    feature: ModelFeature,
    idx: Int,
    extractType: DataType,
    columns: Seq[BinColumn]
  ): Option[(Int, Double)] = {

    // If input value is null just skip it
    if (row.isNullAt(idx)) return None

    // Get numeric representation of extracted feature
    val value = extractType match {
      case ShortType => row.getShort(idx).toDouble
      case IntegerType => row.getInt(idx).toDouble
      case LongType => row.getLong(idx).toDouble
      case DoubleType => row.getDouble(idx)
      case tpe => sys.error(s"Unsupported bin extract type: $tpe. Feature: ${feature.feature}. Columns: ${columns.size}")
    }

    columns.filter(_.fallIntoThisBin(value)).map(_.columnId).map((_, 1.0D)).headOption
  }

  private def formatFeatureErrors(errors: Seq[FeaturizationError]): String = {
    val out = errors.map { case e => s" - Feature: ${e.feature}. Error: ${e.errorMessage}" }
    out.mkString(System.lineSeparator())
  }

}
