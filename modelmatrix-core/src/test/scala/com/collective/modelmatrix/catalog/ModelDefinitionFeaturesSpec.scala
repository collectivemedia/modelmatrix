package com.collective.modelmatrix.catalog

import java.time.Instant

import com.collective.modelmatrix.ModelFeature
import com.collective.modelmatrix.transform.Transform.{Index, Top, Identity}
import org.scalatest.{GivenWhenThen, BeforeAndAfterAll, FlatSpec}

class H2ModelDefinitionFeaturesSpec extends ModelDefinitionFeaturesSpec with H2Database

trait ModelDefinitionFeaturesSpec extends FlatSpec with GivenWhenThen with BeforeAndAfterAll with CatalogDatabase {

  import scala.concurrent.ExecutionContext.Implicits.global

  val now = Instant.now()
  val isActive = true
  val addAllOther = true

  lazy val modelDefinitions = new ModelDefinitions(schema)
  lazy val modelDefinitionFeatures = new ModelDefinitionFeatures(schema)

  "Model Definition Features" should "add model definition features and read them later" in {

    Given("model features")
    val identity = ModelFeature(isActive, "Advertisement", "ad_size", "size", Identity)
    val top = ModelFeature(isActive, "Advertisement", "ad_type", "type", Top(95, addAllOther))
    val index = ModelFeature(isActive, "Advertisement", "ad_network", "network", Index(0.5, addAllOther))

    And("model definition")
    val addModelDefinition = modelDefinitions.add(
      name = None,
      source = "source",
      createdBy = "ModelDefinitionFeaturesSpec",
      createdAt = now,
      comment = Some("testing")
    )

    Then("should save model and features in catalog")

    val insert = for {
      id <- addModelDefinition
      featureId <- modelDefinitionFeatures.addFeatures(id, identity, top, index)
    } yield (id, featureId)

    val (modelDefinitionId, featuresId) = await(db.run(insert))
    assert(featuresId.size == 3)

    And("read all features back by model definition id")

    val features = await(db.run(modelDefinitionFeatures.modelFeatures(modelDefinitionId)))

    assert(features.size == 3)

    val featureMap = features.map(f => f.feature.feature -> f.feature).toMap

    assert(featureMap("ad_size") == identity)
    assert(featureMap("ad_type") == top)
    assert(featureMap("ad_network") == index)
  }

}
