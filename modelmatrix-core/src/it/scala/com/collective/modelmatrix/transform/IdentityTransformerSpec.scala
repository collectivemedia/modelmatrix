package com.collective.modelmatrix.transform

import com.collective.modelmatrix.{ModelFeature, TestSparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.FlatSpec

import scalaz.syntax.either._

class IdentityTransformerSpec extends FlatSpec with TestSparkContext {

  val sqlContext = new SQLContext(sc)

  val schema = StructType(Seq(
    StructField("ad_site", StringType),
    StructField("ad_id", IntegerType)
  ))

  val input = Seq(
      Row("cnn.com", 1),
      Row("bbc.com", 2),
      Row("hbo.com", 1),
      Row("mashable.com", 3)
  )

  val isActive = true
  val withAllOther = true

  val adSite = ModelFeature(isActive, "Ad", "ad_site", "ad_site", Identity)
  val adId = ModelFeature(isActive, "Ad", "ad_id", "ad_id", Identity)

  val transformer = new IdentityTransformer(sqlContext.createDataFrame(sc.parallelize(input), schema))

  "Identity Transformer" should "support integer typed model feature" in {
    val valid = transformer.validate(adId)
    assert(valid == TypedModelFeature(adId, IntegerType).right)
  }

  it should "fail if column doesn't exists" in {
    val failed = transformer.validate(adSite.copy(extract = "ad_site_name"))
    assert(failed == InputSchemaError.ExtractColumnNotFound("ad_site_name").left)
  }

  it should "fail if column type is not supported" in {
    val failed = transformer.validate(adSite)
    assert(failed == InputSchemaError.UnsupportedTransformDataType("ad_site", StringType, Identity).left)
  }

}
