package com.collective.modelmatrix

import java.nio.ByteBuffer

import com.collective.modelmatrix.CategorialColumn.{AllOther, CategorialValue}
import com.collective.modelmatrix.catalog.{ModelInstanceFeature, ModelInstanceIdentityFeature, ModelInstanceIndexFeature, ModelInstanceTopFeature}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Row, DataFrame}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import scodec.bits.ByteVector

import scalaz.{\/-, -\/, \/}
import scalaz.syntax.either._

case class IdentifiedPoint(id: Long, features: Vector)

sealed trait FeatureSchemaError {
  def feature: String
  def errorMessage: String
}

object FeatureSchemaError {

  case class ExtractColumnNotFound(feature: String, extract: String) extends FeatureSchemaError {
    def errorMessage: String = s"Can't find extract column: $extract"
  }

  case class ExtractColumnTypeDoesNotMatch(
    feature: String,
    extract: String,
    expected: DataType,
    found: DataType
  ) extends FeatureSchemaError {
    def errorMessage: String = s"Extract column: $extract type: $found doesn't match expected: $expected"
  }
}

class FeatureExtraction(features: Seq[ModelInstanceFeature]) extends Serializable {

  private val log = LoggerFactory.getLogger(classOf[ModelInstanceFeature])

  import FeatureSchemaError._

  // Check that all input features belong to the same model instance
  private val instances = features.map(_.modelInstanceId).toSet
  require(instances.size == 1, s"Features belong to different model instances: $instances")

  // Maximum columns id in instance features
  private val totalNumberOfColumns = features.flatMap {
    case ModelInstanceIdentityFeature(_, _, _, _, columnId) => Seq(columnId)
    case ModelInstanceTopFeature(_, _, _, _, cols) => cols.map(_.columnId)
    case ModelInstanceIndexFeature(_, _, _, _, cols) => cols.map(_.columnId)
  }.max

  type FeatureColumnId = (ModelFeature, Int)

  def validate(input: DataFrame): Seq[FeatureSchemaError \/ Column] = {

    def inputDataType(expression: String): Option[DataType] = {
      input.schema.find(_.name == expression).map(_.dataType)
    }

    features.map {
      // Valid features
      case ModelInstanceIdentityFeature(_, _, feature, extractType, _)
        if inputDataType(feature.extract).exists(_ == extractType) => new Column(feature.extract).right
      case ModelInstanceTopFeature(_, _, feature, extractType, _)
        if inputDataType(feature.extract).exists(_ == extractType) => new Column(feature.extract).right
      case ModelInstanceIndexFeature(_, _, feature, extractType, _)
        if inputDataType(feature.extract).exists(_ == extractType) => new Column(feature.extract).right

      // Validation errors
      case f: ModelInstanceFeature if inputDataType(f.feature.extract).isEmpty =>
        ExtractColumnNotFound(f.feature.feature, f.feature.extract).left
      case f: ModelInstanceFeature =>
        ExtractColumnTypeDoesNotMatch(
          f.feature.feature, 
          f.feature.extract, 
          inputDataType(f.feature.extract).get, 
          f.extractType
        ).left
    }
  }

  def featurize(input: DataFrame, idColumn: String): RDD[IdentifiedPoint] = {
    log.info(s"Extract features from input DataFrame with id column: $idColumn. Total number of columns: $totalNumberOfColumns")

    // Check that schema satisfies input data
    val validationErrors = validate(input).collect { case -\/(error) => error }
    require(validationErrors.isEmpty,
      s"\nFound ${validationErrors.size} input data errors: \n ${formatFeatureErrors(validationErrors)}")

    // Check that id columns exists and has correct type
    require(input.schema.fields.exists(field => field.name == idColumn && field.dataType == LongType),
      s"Can't find id column: $idColumn with compatible type")

    // Select only columns that are used in feature building
    val columns = validate(input).collect { case \/-(column) => column }
    
    val featuresColumnIdx: Map[ModelFeature, Int] =
      features.zipWithIndex.map { case (f, idx) => (f.feature, idx + 1) }.toMap

    input.select(new Column(idColumn) +: columns:_*).map { row =>
      val id = row.getLong(0)
      val columnValues = features.flatMap {
        case ModelInstanceIdentityFeature(_, _, f, tpe, columnId) => 
          identityColumn(row)(f, featuresColumnIdx(f), tpe, columnId) :: Nil
        case ModelInstanceTopFeature(_, _, f, tpe, cols) =>
          categorialColumn(row)(f, featuresColumnIdx(f), tpe, cols).toSeq
        case ModelInstanceIndexFeature(_, _, f, tpe, cols) =>
          categorialColumn(row)(f, featuresColumnIdx(f), tpe, cols).toSeq
      }

      // Column values 1-based and Vector values are 0-based
      val vectorValues = columnValues.map { case (idx, v) => (idx - 1, v) }

      IdentifiedPoint(id, Vectors.sparse(totalNumberOfColumns, vectorValues))
    }
  }


  private def identityColumn(row: Row)(feature: ModelFeature, idx: Int, extractType: DataType, columnId: Int): (Int, Double) = {
    val doubleValue = extractType match {
      case ShortType => row.getShort(idx).toDouble
      case IntegerType => row.getInt(idx).toDouble
      case LongType => row.getLong(idx).toDouble
      case DoubleType => row.getDouble(idx)
      case tpe => sys.error(s"Unsupported identity extract type: $tpe. Feature: ${feature.feature}")
    }
    (idx, doubleValue)
  }
  
  private def categorialColumn(row: Row)(
    feature: ModelFeature,
    idx: Int,
    extractType: DataType,
    columns: Seq[CategorialColumn]
  ): Option[(Int, Double)] = {

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

  private def formatFeatureErrors(errors: Seq[FeatureSchemaError]): String = {
    val out = errors.map { case e => s" - Feature: ${e.feature}. Error: ${e.errorMessage}" }
    out.mkString(System.lineSeparator())
  }

}
