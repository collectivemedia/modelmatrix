package com.collective.modelmatrix.transform

import com.collective.modelmatrix.ModelFeature
import com.collective.modelmatrix.transform.FeatureTransformationError.{FeatureColumnNotFound, UnsupportedTransformDataType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scalaz._
import scalaz.syntax.either._

class IdentityTransformer(input: DataFrame @@ Transformer.Features) extends Transformer(input) {

  private val supportedDataTypes = Seq(ShortType, IntegerType, LongType, DoubleType)

  def validate: PartialFunction[ModelFeature, \/[FeatureTransformationError, TypedModelFeature]] = {
    case f@ModelFeature(_, _, _, _, Identity) if featureDataType(f.feature).isEmpty =>
      FeatureColumnNotFound(f.feature).left

    case f@ModelFeature(_, _, _, _, Identity)
      if featureDataType(f.feature).isDefined && supportedDataTypes.contains(featureDataType(f.feature).get) =>
      TypedModelFeature(f, featureDataType(f.feature).get).right

    case f@ModelFeature(_, _, _, _, t@Identity) =>
      UnsupportedTransformDataType(f.feature, featureDataType(f.feature).get, t).left
  }

}
