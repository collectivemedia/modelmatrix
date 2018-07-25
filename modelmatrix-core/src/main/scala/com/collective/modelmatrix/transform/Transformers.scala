package com.collective.modelmatrix.transform

import com.collective.modelmatrix.ModelFeature
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import scalaz._

trait Transformers {

  protected class Transformers(input: DataFrame @@ Transformer.Features)(implicit session: SparkSession) {

    val identity = new IdentityTransformer(input)
    val top = new TopTransformer(input)
    val index = new IndexTransformer(input)
    val bins = new BinsTransformer(input)

    private val unknownFeature: PartialFunction[ModelFeature, FeatureTransformationError \/ TypedModelFeature] = {
      case feature => sys.error(s"Feature can't be validated by any of transformers: $feature")
    }

    def validate(feature: ModelFeature): FeatureTransformationError \/ TypedModelFeature =
      (identity.validate orElse
        top.validate orElse
        index.validate orElse
        bins.validate orElse
        unknownFeature
        )(feature)

  }
}
