package com.collective.modelmatrix.transform

import com.collective.modelmatrix.{ModelFeature, ModelMatrixAccess, TestSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

import scala.util.Random
import scalaz.syntax.either._
import scalaz.{-\/, \/-}

class BinsTransformerSpec extends FlatSpec with TestSparkContext {

  val sqlContext = ModelMatrixAccess.sqlContext(sc)

  val schema = StructType(Seq(
    StructField("adv_site", StringType),
    StructField("pct_click", DoubleType)
  ))

  val rnd = new Random()

  def rand(p: Double): Double = {
    p + rnd.nextInt(100).toDouble / 10000
  }

  val input =
    Seq.fill(20)(Row("cnn.com", rand(0.5))) ++
    Seq.fill(20)(Row("bbc.com", rand(0.6))) ++
    Seq.fill(20)(Row("hbo.com", rand(0.7))) ++
    Seq.fill(20)(Row("mashable.com", rand(0.8))) ++
    Seq.fill(20)(Row("reddit.com", rand(0.9))) ++
    Seq.fill(20)(Row("ycombinator.com", rand(1.0)))


  val isActive = true
  val withAllOther = true

  val adSite = ModelFeature(isActive, "Ad", "ad_site", "adv_site", Bins(3, 0, 0))
  val sitePerformance = ModelFeature(isActive, "Site", "site_performance", "pct_click", Bins(3, 0, 0))

  val df = sqlContext.createDataFrame(sc.parallelize(input), schema)
  val transformer = new BinsTransformer(Transformer.extractFeatures(df, Seq(adSite, sitePerformance)) match {
    case -\/(err) => sys.error(s"Can't extract features: $err")
    case \/-(suc) => suc
  })

  "Bins Transformer" should "support integer typed model feature" in {
    val valid = transformer.validate(sitePerformance)
    assert(valid == TypedModelFeature(sitePerformance, DoubleType).right)

    val typed = valid.toOption.get
    val columns = transformer.transform(typed)
    assert(columns.size == 3)
  }

  it should "fail if feature column doesn't exists" in {
    val failed = transformer.validate(sitePerformance.copy(feature = "site_clicks"))
    assert(failed == FeatureTransformationError.FeatureColumnNotFound("site_clicks").left)
  }

  it should "fail if column type is not supported" in {
    val failed = transformer.validate(adSite)
    assert(failed == FeatureTransformationError.UnsupportedTransformDataType("ad_site", StringType, Bins(3, 0, 0)).left)
  }

}
