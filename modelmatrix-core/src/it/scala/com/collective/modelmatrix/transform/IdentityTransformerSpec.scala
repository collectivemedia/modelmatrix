package com.collective.modelmatrix.transform

import com.collective.modelmatrix.{ModelFeature, ModelMatrix, TestSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FlatSpec

import scalaz.syntax.either._
import scalaz.{-\/, \/-}

class IdentityTransformerSpec extends FlatSpec with TestSparkContext {

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

  val adSite = ModelFeature(isActive, "Ad", "ad_site", "adv_site", Identity)
  val adId = ModelFeature(isActive, "Ad", "ad_id", "adv_id", Identity)

  val df = session.createDataFrame(session.sparkContext.parallelize(input), schema)
  val transformer = new IdentityTransformer(Transformer.extractFeatures(df, Seq(adSite, adId)) match {
    case -\/(err) => sys.error(s"Can't extract features: $err")
    case \/-(suc) => suc
  })

  "Identity Transformer" should "support integer typed model feature" in {
    val valid = transformer.validate(adId)
    assert(valid == TypedModelFeature(adId, IntegerType).right)
  }

  it should "fail if feature column doesn't exists" in {
    val failed = transformer.validate(adSite.copy(feature = "adv_site"))
    assert(failed == FeatureTransformationError.FeatureColumnNotFound("adv_site").left)
  }

  it should "fail if column type is not supported" in {
    val failed = transformer.validate(adSite)
    assert(failed == FeatureTransformationError.UnsupportedTransformDataType("ad_site", StringType, Identity).left)
  }

}
