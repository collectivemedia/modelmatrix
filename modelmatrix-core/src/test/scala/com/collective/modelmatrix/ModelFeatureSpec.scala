package com.collective.modelmatrix

import com.collective.modelmatrix.transform.{Bins, Identity, Index, Top}
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec

import scalaz.syntax.validation._

class ModelFeatureSpec extends FlatSpec {

  private val isActive = true
  private val notActive = false

  private val isAllOther = true
  private val features = ConfigFactory.load("./matrix-model.conf").getConfig("features")

  "Model Feature" should "parse 'identity' feature" in {
    val adNetwork = ModelFeature.parse("ad_network", features.getConfig("ad_network"))
    assert(adNetwork == ModelFeature(
      isActive,
      "advertisement",
      "ad_network",
      "network",
      Identity
    ).successNel)
  }

  it should "parse 'top' feature" in {
    val adType = ModelFeature.parse("ad_type", features.getConfig("ad_type"))
    assert(adType == ModelFeature(
      isActive,
      "advertisement",
      "ad_type",
      "type",
      Top(95.0, isAllOther)
    ).successNel)
  }

  it should "parse 'index' feature" in {
    val adSize = ModelFeature.parse("ad_size", features.getConfig("ad_size"))
    assert(adSize == ModelFeature(
      isActive,
      "advertisement",
      "ad_size",
      "size",
      Index(0.5, isAllOther)
    ).successNel)
  }

  it should "parse 'bins' feature" in {
    val adPerformance = ModelFeature.parse("ad_performance", features.getConfig("ad_performance"))
    assert(adPerformance == ModelFeature(
      isActive,
      "performance",
      "ad_performance",
      "pct_clicks",
      Bins(10, 100, 1.0)
    ).successNel)
  }

  it should "parse deactivated feature" in {
    val adVisibility = ModelFeature.parse("ad_visibility", features.getConfig("ad_visibility"))
    assert(adVisibility == ModelFeature(
      notActive,
      "advertisement",
      "ad_visibility",
      "visibility",
      Top(95.0, isAllOther)
    ).successNel)
  }

  it should "fail on wrong transformation type" in {
    val adTag = ModelFeature.parse("ad_tag", features.getConfig("ad_tag"))
    assert(adTag == "Unknown transform type: magic-transform".failureNel)
  }

  it should "fail on wrong transformation parameter" in {
    val adPosition = ModelFeature.parse("ad_position", features.getConfig("ad_position"))
    assert(adPosition.isFailure)
  }

}
