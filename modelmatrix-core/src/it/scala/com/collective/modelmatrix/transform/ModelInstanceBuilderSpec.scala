package com.collective.modelmatrix.transform

import com.collective.modelmatrix.{ModelFeature, TestSparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.FlatSpec

import scalaz.syntax.validation._

class ModelInstanceBuilderSpec extends FlatSpec with TestSparkContext {

  val sqlContext = new SQLContext(sc)

  val schema = StructType(Seq(
    StructField("ad_id", LongType),
    StructField("ad_network", IntegerType),
    StructField("ad_site", StringType)
  ))

  val input = Seq(
    Row(1l, 123, "cnn.com"),
    Row(1l, 123, "bbc.com"),
    Row(2l, 456, "hbo.com")
  )

  val isActive = true
  val withAllOther = true

  val adId = ModelFeature(isActive, "Ad", "ad_id", "ad_id", Identity)
  val adNetwork = ModelFeature(isActive, "Ad", "ad_network", "ad_network", Top(0.5, withAllOther))
  val adSite = ModelFeature(isActive, "Ad", "ad_site", "ad_site", Index(0.5, withAllOther))

  val df: DataFrame = sqlContext.createDataFrame(sc.parallelize(input), schema)

  val builder = new ModelInstanceBuilder

  "Model Instance Builder" should "successfully validate DataFrame schema" in {
    val typed = builder.validateInput(df, Seq(adId, adNetwork, adSite))

    assert(typed.size == 3)

    assert(typed(0) == builder.TypedModelFeature(adId, LongType).successNel)
    assert(typed(1) == builder.TypedModelFeature(adNetwork, IntegerType).successNel)
    assert(typed(2) == builder.TypedModelFeature(adSite, StringType).successNel)
  }

  it should "fail on validation string identity feature" in {
    // String identity transformation is not supported
    val wrongAdSite = ModelFeature(isActive, "Ad", "ad_site", "ad_site", Identity)
    val typed = builder.validateInput(df, wrongAdSite)
    assert(typed == InputSchemaError.UnsupportedTransformDataType("ad_site", StringType, Identity).failureNel)
  }

  it should "fail on non-existing extract column" in {
    // String identity transformation is not supported
    val adTitle = ModelFeature(isActive, "Ad", "ad_title", "ad_title", Identity)
    val typed = builder.validateInput(df, adTitle)
    assert(typed == InputSchemaError.ExtractColumnNotFound("ad_title").failureNel)
  }

}
