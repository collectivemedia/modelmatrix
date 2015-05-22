package com.collective.modelmatrix.catalog

import java.time.Instant

import com.collective.modelmatrix.{CategorialColumn, ModelFeature}
import com.collective.modelmatrix.transform.{Index, Top, Identity}
import org.apache.spark.sql.types.{StringType, IntegerType}
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen, FlatSpec}
import scodec.bits.ByteVector

class H2ModelInstanceCatalogSpec extends ModelInstanceCatalogSpec with H2Database

trait ModelInstanceCatalogSpec extends FlatSpec with GivenWhenThen with BeforeAndAfterAll with CatalogDatabase {

  import scala.concurrent.ExecutionContext.Implicits.global

  val now = Instant.now()
  val isActive = true
  val addAllOther = true

  // Features
  val identity = ModelFeature(isActive, "Advertisement", "ad_size", "size", Identity)
  val top = ModelFeature(isActive, "Advertisement", "ad_type", "type", Top(95, addAllOther))
  val index = ModelFeature(isActive, "Advertisement", "ad_network", "network", Index(0.5, addAllOther))

  // Definitions
  lazy val modelDefinitions = new ModelDefinitions(catalog)
  lazy val modelDefinitionFeatures = new ModelDefinitionFeatures(catalog)

  // Instances
  lazy val modelInstances = new ModelInstances(catalog)
  lazy val modelInstanceFeatures = new ModelInstanceFeatures(catalog)

  // Create model definition first
  def newModelDefinition = {

    val createModelDefinition = modelDefinitions.add(
      name = Some(s"definition=${now.toEpochMilli}"),
      source = "source",
      createdBy = "ModelInstanceCatalogSpec",
      createdAt = now,
      comment = None
    )

    val insert = for {
      id <- createModelDefinition
      featureId <- modelDefinitionFeatures.addFeatures(id, identity, top, index)
    } yield (id, featureId)

    await(db.run(insert))
  }

  "Model Instances Catalog" should "add identity feature" in {

    Given("model definition")
    val (modelDefinitionId, Seq(identityFeatureId, topFeatureId, indexFeatureId)) = newModelDefinition

    Then("should create model instance")

    val createModelInstance = modelInstances.add(
      modelDefinitionId,
      name = Some(s"instance=${now.toEpochMilli}"),
      createdBy = "ModelInstanceCatalogSpec",
      createdAt = now,
      comment = None
    )

    val topColumns = Seq(
      CategorialColumn.CategorialValue(2, "banner", ByteVector("banner".getBytes), 100, 100),
      CategorialColumn.CategorialValue(3, "mobile", ByteVector("mobile".getBytes), 200, 300),
      CategorialColumn.AllOther(4, 100, 400)
    )

    val indexColumns = Seq(
      CategorialColumn.CategorialValue(5, "google", ByteVector("google".getBytes), 100, 100),
      CategorialColumn.CategorialValue(6, "yahoo", ByteVector("yahoo".getBytes), 200, 300),
      CategorialColumn.AllOther(7, 100, 400)
    )

    val insert = for {
      id <- createModelInstance
      _ <- modelInstanceFeatures.addIdentityFeature(id, identityFeatureId, IntegerType, 1)
      _ <- modelInstanceFeatures.addTopFeature(id, topFeatureId, StringType, topColumns)
      _ <- modelInstanceFeatures.addIndexFeature(id, indexFeatureId, StringType, indexColumns)
    } yield id

    val modelInstanceId = await(db.run(insert))

    And("find model instance in database")
    val instanceO = await(db.run(modelInstances.all)).find(_.id == modelInstanceId)
    assert(instanceO.isDefined)

    val instance = instanceO.get
    assert(instance.createdAt == now)
    assert(instance.features == 3)
    assert(instance.columns == 7)

    And("find model instance by id")
    val foundById = await(db.run(modelInstances.findById(modelInstanceId)))
    assert(foundById == instanceO)

    And("find model instance by name")
    val foundByName = await(db.run(modelInstances.findByName(s"instance=${now.toEpochMilli}"))).headOption
    assert(foundByName == instanceO)

    And("read model instance columns")
    val features = await(db.run(modelInstanceFeatures.features(modelInstanceId)))
    val featureMap = features.map(f => f.feature.feature -> f).toMap
    assert(features.size == 3)

    And("get valid identity columns")
    assert(featureMap("ad_size").isInstanceOf[ModelInstanceIdentityFeature])
    assert(featureMap("ad_size").feature == identity)
    assert(featureMap("ad_size").extractType == IntegerType)
    assert(featureMap("ad_size").asInstanceOf[ModelInstanceIdentityFeature].columnId === 1)

   And("get valid top columns")
    assert(featureMap("ad_type").isInstanceOf[ModelInstanceTopFeature])
    assert(featureMap("ad_type").feature == top)
    assert(featureMap("ad_type").extractType == StringType)
    assert(featureMap("ad_type").asInstanceOf[ModelInstanceTopFeature].columns == topColumns)

    And("get valid index columns")
    assert(featureMap("ad_network").isInstanceOf[ModelInstanceIndexFeature])
    assert(featureMap("ad_network").feature == index)
    assert(featureMap("ad_network").extractType == StringType)
    assert(featureMap("ad_network").asInstanceOf[ModelInstanceIndexFeature].columns == indexColumns)
  }

}
