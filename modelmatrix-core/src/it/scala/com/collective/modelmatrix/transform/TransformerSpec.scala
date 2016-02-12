package com.collective.modelmatrix.transform

import com.collective.modelmatrix.{ModelFeature, ModelMatrix, TestSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FlatSpec

class TransformerSpec extends FlatSpec with TestSparkContext {

  val sqlContext = ModelMatrix.sqlContext(sc)

  val schema = StructType(Seq(
    StructField("adv_site", StringType),
    StructField("adv_id", IntegerType)
  ))

  val input = Seq(
    Row("cnn.com", 1),
    Row("bbc.com", 2),
    Row("hbo.com", 1),
    Row("mashable.com", 3)
  )

  val isActive = true
  val withAllOther = true

  // Not enough parameters for 'concat'
  val wrongParametersCount = ModelFeature(isActive, "advertisement", "f2", "concat(adv_site)", Top(95.0, allOther = false))

  val df = sqlContext.createDataFrame(sc.parallelize(input), schema)

  "Transformer" should "report failed feature extraction" in {
    val features = Transformer.extractFeatures(df, Seq(wrongParametersCount))
    assert(features.isLeft)
    val errors = features.fold(identity, _ => sys.error("Should not be here"))

    assert(errors.length == 1)
    assert(errors(0).feature == wrongParametersCount)
  }

}
