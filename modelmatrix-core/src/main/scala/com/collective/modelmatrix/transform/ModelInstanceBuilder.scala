package com.collective.modelmatrix.transform

import com.collective.modelmatrix.ModelFeature
import com.collective.modelmatrix.transform.InputSchemaError.{ExtractColumnNotFound, UnsupportedTransformDataType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scalaz.ValidationNel
import scalaz.syntax.validation._

class ModelInstanceBuilder {

  case class TypedModelFeature(feature: ModelFeature, extractType: DataType)

  private object ValidationRules {

    type ValidationRule = PartialFunction[(ModelFeature, Option[DataType]), ValidationNel[InputSchemaError, TypedModelFeature]]

    // Check that extract column exists
    val extractColumnExists: ValidationRule = {
      case (feature, None) => ExtractColumnNotFound(feature.extract).failureNel
    }

    // Check that 'identity' feature supported
    val validIdentityFeature: ValidationRule = {
      case (f@ModelFeature(_, _, _, e, Identity), tpe@Some(ShortType | IntegerType | LongType | DoubleType)) =>
        TypedModelFeature(f, tpe.get).successNel

      case (f@ModelFeature(_, _, _, e, t@Identity), Some(otherType)) =>
        UnsupportedTransformDataType(e, otherType, t).failureNel
    }

    // Check that 'top' feature supported
    val validTopFeature: ValidationRule = {
      case (f@ModelFeature(_, _, _, e, Top(_, _)), tpe@Some(ShortType | IntegerType | LongType | DoubleType | StringType)) =>
        TypedModelFeature(f, tpe.get).successNel

      case (f@ModelFeature(_, _, _, e, t@Top(_, _)), Some(otherType)) =>
        UnsupportedTransformDataType(e, otherType, t).failureNel
    }

    // Check that 'index' feature supported
    val validIndexFeature: ValidationRule = {
      case (f@ModelFeature(_, _, _, e, Index(_, _)), tpe@Some(ShortType | IntegerType | LongType | DoubleType | StringType)) =>
        TypedModelFeature(f, tpe.get).successNel

      case (f@ModelFeature(_, _, _, e, t@Index(_, _)), Some(otherType)) =>
        UnsupportedTransformDataType(e, otherType, t).failureNel
    }

    // If model feature didn't match any previous rule it's definitely some error
    val validationError: ValidationRule = {
      case (f, dt) => sys.error(s"Can't validate feature definition: $f")
    }
  }

  def validateInput(input: DataFrame, features: ModelFeature): ValidationNel[InputSchemaError, TypedModelFeature] = {
    validateInput(input, Seq(features)).head
  }

  def validateInput(input: DataFrame, features: Seq[ModelFeature]): Seq[ValidationNel[InputSchemaError, TypedModelFeature]] = {
    import ValidationRules._

    def dataType(name: String): Option[DataType] = {
      input.schema.fields.find(_.name == name).map(_.dataType)
    }

    features.map(f => (f, dataType(f.extract))).map(
      extractColumnExists orElse
      validIdentityFeature orElse
      validTopFeature orElse
      validIndexFeature orElse
      validationError
    )
  }
}
