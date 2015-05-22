package com.collective.modelmatrix.catalog

import java.time.Instant

import com.collective.modelmatrix.ModelFeature
import com.collective.modelmatrix.transform.{Index, Top, Identity}
import org.scalatest.{GivenWhenThen, BeforeAndAfterAll, FlatSpec}

class H2ModelDefinitionCatalogSpec extends ModelDefinitionCatalogSpec with H2Database

trait ModelDefinitionCatalogSpec extends FlatSpec with GivenWhenThen with BeforeAndAfterAll with CatalogDatabase {

  import scala.concurrent.ExecutionContext.Implicits.global

  val now = Instant.now()
  val isActive = true
  val addAllOther = true

  lazy val modelDefinitions = new ModelDefinitions(catalog)
  lazy val modelDefinitionFeatures = new ModelDefinitionFeatures(catalog)

  "Model Definition Catalog" should "add model definition with features and read them later" in {

    Given("model features")
    val identity = ModelFeature(isActive, "Advertisement", "ad_size", "size", Identity)
    val top = ModelFeature(isActive, "Advertisement", "ad_type", "type", Top(95, addAllOther))
    val index = ModelFeature(isActive, "Advertisement", "ad_network", "network", Index(0.5, addAllOther))

    And("model definition")
    val addModelDefinition = modelDefinitions.add(
      name = Some(s"name=${now.toEpochMilli}"),
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

    And("read saved model")

    val modelO = await(db.run(modelDefinitions.all)).find(_.id == modelDefinitionId)
    assert(modelO.isDefined)

    val model = modelO.get
    assert(model.createdBy == "ModelDefinitionFeaturesSpec")
    assert(model.createdAt == now)
    assert(model.features == 3)

    And("find model definitions by id")
    val foundById = await(db.run(modelDefinitions.findById(modelDefinitionId)))
    assert(foundById == modelO)

    And("find model definition by name")
    val foundByName = await(db.run(modelDefinitions.findByName(s"name=${now.toEpochMilli}"))).headOption
    assert(foundByName == modelO)

    And("read all model features by model definition id")
    val features = await(db.run(modelDefinitionFeatures.features(modelDefinitionId)))
    val featureMap = features.map(f => f.feature.feature -> f.feature).toMap

    assert(features.size == 3)
    assert(featureMap("ad_size") == identity)
    assert(featureMap("ad_type") == top)
    assert(featureMap("ad_network") == index)
  }

}
