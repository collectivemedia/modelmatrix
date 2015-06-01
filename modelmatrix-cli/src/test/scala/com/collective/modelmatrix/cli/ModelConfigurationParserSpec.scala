package com.collective.modelmatrix.cli

import com.collective.modelmatrix.ModelFeature
import com.collective.modelmatrix.transform.{Identity, Index, Top}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, GivenWhenThen}

import scalaz.syntax.validation._

class ModelConfigurationParserSpec extends FlatSpec with GivenWhenThen {

  val isActive = true
  val notActive = false
  val isAllOther = true

  "Model Configuration parser" should "parse model matrix config" in {

    Given("well-defined model-matrix config")
    val config = ConfigFactory.load("./matrix-model.conf")
    val parser = new ModelConfigurationParser(config)

    Then("should parse all features")
    val features = parser.features().toMap

    assert(features.size == 6)

    And("ad_network should be correct 'identity' feature")
    val adNetwork = features("ad_network")
    assert(adNetwork == ModelFeature(
      isActive,
      "advertisement",
      "ad_network",
      "network",
      Identity
    ).successNel)

    And("ad_type should be correct 'top' feature")
    val adType = features("ad_type")
    assert(adType == ModelFeature(
      isActive,
      "advertisement",
      "ad_type",
      "type",
      Top(95.0, isAllOther)
    ).successNel)

    And("ad_size should be correct 'index' feature")
    val adSize = features("ad_size")
    assert(adSize == ModelFeature(
      isActive,
      "advertisement",
      "ad_size",
      "size",
      Index(0.5, isAllOther)
    ).successNel)

    And("ad_visibility should be correct 'top' feature")
    val adVisibility = features("ad_visibility")
    assert(adVisibility == ModelFeature(
      notActive,
      "advertisement",
      "ad_visibility",
      "visibility",
      Top(95.0, isAllOther)
    ).successNel)

    And("ad_tag should be wrong-defined feature")
    val adTag = features("ad_tag")
    assert(adTag == "Unknown transform type: magic-transform".failureNel)

    And("ad_position should be wrong-defined feature")
    val adPosition = features("ad_position")
    assert(adPosition.isFailure)
  }

}
