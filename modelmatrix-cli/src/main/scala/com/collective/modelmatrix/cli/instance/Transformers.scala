package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.ModelFeature
import com.collective.modelmatrix.transform._
import org.apache.spark.sql.{DataFrame, SQLContext}

import scalaz.\/

trait Transformers {

  protected class Transformers(input: DataFrame)(implicit sqlContext: SQLContext) {

    val identity = new IdentityTransformer(input)
    val top = new TopTransformer(input)
    val index = new IndexTransformer(input)
    val bins = new BinsTransformer(input)

    private val unknownFeature: PartialFunction[ModelFeature, TransformSchemaError \/ TypedModelFeature] = {
      case feature => sys.error(s"Feature can't be validated by any of transformers: $feature")
    }

    def validate(feature: ModelFeature): TransformSchemaError \/ TypedModelFeature =
      (identity.validate orElse
       top.validate orElse
       index.validate orElse
       bins.validate orElse
       unknownFeature
      )(feature)

  }
}
