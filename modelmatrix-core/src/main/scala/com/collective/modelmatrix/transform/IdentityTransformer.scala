package com.collective.modelmatrix.transform

import com.collective.modelmatrix.ModelFeature
import com.collective.modelmatrix.transform.TransformSchemaError.{ExtractColumnNotFound, UnsupportedTransformDataType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scalaz.\/
import scalaz.syntax.either._

class IdentityTransformer(input: DataFrame) extends Transformer(input) {

  private val supportedDataTypes = Seq(ShortType, IntegerType, LongType, DoubleType)

  def validate: PartialFunction[ModelFeature, \/[TransformSchemaError, TypedModelFeature]] = {
    case f@ModelFeature(_, _, _, e, Identity) if inputDataType(e).isEmpty =>
      ExtractColumnNotFound(e).left

    case f@ModelFeature(_, _, _, e, Identity)
      if inputDataType(e).isDefined && supportedDataTypes.contains(inputDataType(e).get) =>
      TypedModelFeature(f, inputDataType(e).get).right

    case f@ModelFeature(_, _, _, e, t@Identity) =>
      UnsupportedTransformDataType(e, inputDataType(e).get, t).left
  }

}
