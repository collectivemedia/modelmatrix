package com.collective.modelmatrix

import java.nio.ByteBuffer

import com.collective.modelmatrix.CategorialColumn.{AllOther, CategorialValue}
import com.collective.modelmatrix.FeaturizationType.{Identified, Labeled}
import com.collective.modelmatrix.catalog._
import com.collective.modelmatrix.transform.Transformer.Features
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.slf4j.LoggerFactory
import scodec.bits.ByteVector

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

case class IdentifiedPoint(id: Any, features: Vector)

object FeaturizationType {

  case class Labeled(labelColumn: String)

  case class Identified(idColumn: String)

}

class Featurization(features: Seq[ModelInstanceFeature]) extends Serializable {

  private val log = LoggerFactory.getLogger(classOf[ModelInstanceFeature])

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

  def validate(input: DataFrame @@ Features): Seq[FeaturizationError \/ Column] = {

    def featureDataType(feature: String): Option[DataType] = {
      scalaz.Tag.unwrap(input).schema.find(_.name == feature).map(_.dataType)
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
        categorialColumn(row)(f, featuresColumnIdx(f), tpe, cols).toSeq
      case ModelInstanceIndexFeature(_, _, f, tpe, cols) =>
        categorialColumn(row)(f, featuresColumnIdx(f), tpe, cols).toSeq
      case ModelInstanceBinsFeature(_, _, f, tpe, cols) =>
        binColumn(row)(f, featuresColumnIdx(f), tpe, cols).toSeq
    }
  }

  private def validateColumns(input: DataFrame @@ Features): Seq[Column] = {
    val validation = validate(input)
    val errors = validation.collect { case -\/(error) => error }
    require(errors.isEmpty,
      s"\nFound ${errors.size} input data errors: \n ${formatFeatureErrors(errors)}")

    validation.collect { case \/-(column) => column }
  }

  def featurize(input: DataFrame @@ Features, identified: Identified): (DataType, RDD[IdentifiedPoint]) = {
    import identified.idColumn

    log.info(s"Extract identified points from input DataFrame. Id columns: $idColumn. " +
      s"Total number of columns: $totalNumberOfColumns")

    val df = scalaz.Tag.unwrap(input)

    // Check that id columns exists and has correct type
    require(df.schema.fields.exists(_.name == idColumn), s"Can't find id column: $idColumn")
    val idType = df.schema.fields.find(_.name == idColumn).get.dataType

    val rdd = df.select(new Column(idColumn) +: validateColumns(input): _*).map { row =>
      val id = row.get(0)
      val columnValues = readColumns(row, _ + 1)
      // Column values 1-based and Vector values are 0-based
      val vectorValues = columnValues.map { case (idx, v) => (idx - 1, v) }

      IdentifiedPoint(id, Vectors.sparse(totalNumberOfColumns, vectorValues))
    }

    (idType, rdd)
  }

  def featurize(input: DataFrame @@ Features, labeled: Labeled): RDD[LabeledPoint] = {
    import labeled.labelColumn

    log.info(s"Extract labeled points from input DataFrame. Label column: $labelColumn. " +
      s"Total number of columns: $totalNumberOfColumns")

    val df = scalaz.Tag.unwrap(input)

    // Check label column
    val labelColumnType = df.schema.fields.find(_.name == labelColumn).map(_.dataType)
    require(labelColumnType.isDefined, s"Label column '$labelColumn' is not found in data frame")
    require(labelColumnType.get == DoubleType, s"Invalid label column type: ${labelColumnType.get}")

    df.select(new Column(labelColumn) +: validateColumns(input): _*).map { row =>
      val label = row.getDouble(0)
      val columnValues = readColumns(row, _ + 1)
      // Column values 1-based and Vector values are 0-based
      val vectorValues = columnValues.map { case (idx, v) => (idx - 1, v) }

      LabeledPoint(label, Vectors.sparse(totalNumberOfColumns, vectorValues))
    }
  }

  def featurize(input: DataFrame @@ Features): RDD[Vector] = {
    log.info(s"Extract features from input DataFrame. Total number of columns: $totalNumberOfColumns")

    scalaz.Tag.unwrap(input).select(validateColumns(input): _*).map { row =>
      val columnValues = readColumns(row)
      // Column values 1-based and Vector values are 0-based
      val vectorValues = columnValues.map { case (idx, v) => (idx - 1, v) }
      Vectors.sparse(totalNumberOfColumns, vectorValues)
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

    Some((idx, doubleValue))
  }

  private def categorialColumn(row: Row)(
    feature: ModelFeature,
    idx: Int,
    extractType: DataType,
    columns: Seq[CategorialColumn]
  ): Option[(Int, Double)] = {

    // If input value is null just skip it
    if (row.isNullAt(idx)) return None

    // Get byte representation of extracted feature
    val byteVector = extractType match {
      case ShortType => ByteVector(ByteBuffer.allocate(2).putShort(row.getShort(idx)).array())
      case IntegerType => ByteVector(ByteBuffer.allocate(4).putInt(row.getInt(idx)).array())
      case LongType => ByteVector(ByteBuffer.allocate(8).putLong(row.getLong(idx)).array())
      case DoubleType => ByteVector(ByteBuffer.allocate(8).putDouble(row.getDouble(idx)).array())
      case StringType => ByteVector(row.getString(idx).getBytes)
      case tpe => sys.error(s"Unsupported categorial extract type: $tpe. Feature: ${feature.feature}. Columns: ${columns.size}")
    }

    // Take first matching categorial value or fallback to 'all other' if exists
    val categorialColumn = columns.collect { case v: CategorialValue if v.sourceValue == byteVector => v.columnId }
    val allOther = columns.collect { case AllOther(columnId, _, _) => columnId }

    (categorialColumn ++ allOther).headOption.map((_, 1.0D))
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
