package com.collective.modelmatrix

import java.nio.ByteBuffer

import com.collective.modelmatrix.FeatureSchemaError.ExtractColumnTypeDoesNotMatch
import com.collective.modelmatrix.catalog.{ModelInstanceIndexFeature, ModelInstanceTopFeature, ModelInstanceIdentityFeature}
import com.collective.modelmatrix.transform.{Index, Top, Identity}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec
import scodec.bits.ByteVector

import scalaz.\/

class FeatureExtractionSpec extends FlatSpec with TestSparkContext {

  val sqlContext = new SQLContext(sc)

  val schema = StructType(Seq(
    StructField("auction_id", LongType),
    StructField("ad_price", DoubleType),
    StructField("ad_type", IntegerType),
    StructField("ad_site", StringType)
  ))

  val input = Seq(
    Row(1l, 1.23, 1, "cnn.com"),
    Row(2l, 2.34, 2, "bbc.com"),
    Row(3l, 0.12, 2, "hbo.com"),
    Row(4l, 0.09, 3, "mashable.com")
  )

  val isActive = true
  val withAllOther = true

  // Model features
  val adPrice = ModelFeature(isActive, "Ad", "ad_price", "ad_price", Identity)
  val adType = ModelFeature(isActive, "Ad", "ad_type", "ad_type", Top(95.0, withAllOther))
  val adSite = ModelFeature(isActive, "Ad", "ad_site", "ad_site", Index(0.5, withAllOther))

  // Feature instance

  val modelInstanceId = 12345

  val adPriceInstance = ModelInstanceIdentityFeature(1, modelInstanceId, adPrice, DoubleType, 1)

  val adTypeInstance = ModelInstanceTopFeature(2, modelInstanceId, adType, IntegerType, Seq(
    CategorialColumn.CategorialValue(2, "1", ByteVector(ByteBuffer.allocate(4).putInt(1).array()), 100, 100),
    CategorialColumn.CategorialValue(3, "2", ByteVector(ByteBuffer.allocate(4).putInt(2).array()), 100, 200),
    CategorialColumn.AllOther(4, 200, 400)
  ))

  val adSiteInstance = ModelInstanceIndexFeature(3, modelInstanceId, adSite, StringType, Seq(
    CategorialColumn.CategorialValue(5, "cnn.com", ByteVector("cnn.com".getBytes), 100, 100),
    CategorialColumn.CategorialValue(6, "bbc.com", ByteVector("bbc.com".getBytes), 100, 200)
  ))

  val totalColumns = 6

  val df = sqlContext.createDataFrame(sc.parallelize(input), schema)
  val featureExtraction = new FeatureExtraction(Seq(adPriceInstance, adTypeInstance, adSiteInstance))

  "Feature Extraction" should "validate input schema against provided features" in {
    val validation = featureExtraction.validate(df)
    assert(validation.count(_.isRight) == 3)
  }

  it should "fail if extractType doesn't match" in {
    val brokenFeatureExtraction = new FeatureExtraction(Seq(adPriceInstance, adTypeInstance.copy(extractType = DoubleType), adSiteInstance))

    val validation = brokenFeatureExtraction.validate(df)
    assert(validation.count(_.isLeft) == 1)

    val error = validation.find(_.isLeft).head
    assert(error == \/.left(ExtractColumnTypeDoesNotMatch(adType.feature, adType.extract, IntegerType, DoubleType)))
  }

  it should "featurize input data frame" in {

    val featurized = featureExtraction.featurize(df, "auction_id")._2.collect().toSeq.map(p => p.id.asInstanceOf[Long] -> p.features).toMap
    assert(featurized.size == 4)

    // Columns:
    // 1 - adPrice
    // 2 - adType == 1
    // 3 - adType == 2
    // 4 - adType == all other
    // 5 - adSite == cnn.com
    // 6 - adSite == bbc.com

    assert(featurized(1l).asInstanceOf[SparseVector].size == 6)
    assert(featurized(1l).asInstanceOf[SparseVector].indices.toSeq.map(_ + 1) == Seq(1, 2, 5))
    assert(featurized(1l).asInstanceOf[SparseVector].values.toSeq == Seq(1.23, 1.0, 1.0))

    assert(featurized(2l).asInstanceOf[SparseVector].size == 6)
    assert(featurized(2l).asInstanceOf[SparseVector].indices.toSeq.map(_ + 1) == Seq(1, 3, 6))
    assert(featurized(2l).asInstanceOf[SparseVector].values.toSeq == Seq(2.34, 1.0, 1.0))

    assert(featurized(3l).asInstanceOf[SparseVector].size == 6)
    assert(featurized(3l).asInstanceOf[SparseVector].indices.toSeq.map(_ + 1) == Seq(1, 3))
    assert(featurized(3l).asInstanceOf[SparseVector].values.toSeq == Seq(0.12, 1.0))

    assert(featurized(4l).asInstanceOf[SparseVector].size == 6)
    assert(featurized(4l).asInstanceOf[SparseVector].indices.toSeq.map(_ + 1) == Seq(1, 4))
    assert(featurized(4l).asInstanceOf[SparseVector].values.toSeq == Seq(0.09, 1.0))

  }

}
