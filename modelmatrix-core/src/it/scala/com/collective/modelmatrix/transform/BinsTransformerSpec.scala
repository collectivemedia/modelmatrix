package com.collective.modelmatrix.transform

import com.collective.modelmatrix.{ModelFeature, TestSparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

import scalaz.syntax.either._

class BinsTransformerSpec extends FlatSpec with TestSparkContext {

  val sqlContext = new SQLContext(sc)

  val schema = StructType(Seq(
    StructField("adv_site", StringType),
    StructField("pct_click", DoubleType)
  ))

  val input = Seq(
    Row("cnn.com", 0.5),
    Row("bbc.com", 0.6),
    Row("hbo.com", 0.7),
    Row("mashable.com", 0.8),
    Row("reddit.com", 0.9),
    Row("ycombinator.com", 1.0)
  )

  val isActive = true
  val withAllOther = true

  val adSite = ModelFeature(isActive, "Ad", "ad_site", "adv_site", Bins(3, 0, 0))
  val sitePerformance = ModelFeature(isActive, "Site", "site_performance", "pct_click", Bins(3, 0, 0))

  val df = sqlContext.createDataFrame(sc.parallelize(input), schema)
  val transformer = new BinsTransformer(Transformer.selectFeatures(df, Seq(adSite, sitePerformance)))

  "Bins Transformer" should "support integer typed model feature" in {
    val valid = transformer.validate(sitePerformance)
    assert(valid == TypedModelFeature(sitePerformance, DoubleType).right)

    val typed = valid.toOption.get
    val columns = transformer.transform(typed)
    assert(columns.size == 3)
  }

  it should "fail if feature column doesn't exists" in {
    val failed = transformer.validate(sitePerformance.copy(feature = "site_clicks"))
    assert(failed == TransformSchemaError.FeatureColumnNotFound("site_clicks").left)
  }

  it should "fail if column type is not supported" in {
    val failed = transformer.validate(adSite)
    assert(failed == TransformSchemaError.UnsupportedTransformDataType("ad_site", StringType, Bins(3, 0, 0)).left)
  }

}
