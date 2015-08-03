package com.collective.modelmatrix.catalog

import java.time.Instant

import com.collective.modelmatrix.transform.{Bins, Identity, Index, Top}
import com.collective.modelmatrix.{BinColumn, CategoricalColumn, ModelFeature, ModelMatrixEncoding}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen}


class ModelInstanceCatalogSpecTest extends ModelInstanceCatalogSpec with TestDatabase with InstallSchemaBefore


trait ModelInstanceCatalogSpec extends FlatSpec with GivenWhenThen with BeforeAndAfterAll with CatalogDatabase {

  import scala.concurrent.ExecutionContext.Implicits.global

  val now = Instant.now()
  val isActive = true
  val addAllOther = true

  // Features
  val identity = ModelFeature(isActive, "Advertisement", "ad_size", "size", Identity)
  val top = ModelFeature(isActive, "Advertisement", "ad_type", "type", Top(95, addAllOther))
  val index = ModelFeature(isActive, "Advertisement", "ad_network", "network", Index(0.5, addAllOther))
  val bins = ModelFeature(isActive, "Advertisement", "ad_performance", "pct_clicks", Bins(5, 0, 0))

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
      featureId <- modelDefinitionFeatures.addFeatures(id, identity, top, index, bins)
    } yield (id, featureId)

    await(db.run(insert))
  }

  "Model Instances Catalog" should "add model features" in {

    Given("model definition")
    val (modelDefinitionId, Seq(identityFeatureId, topFeatureId, indexFeatureId, binsFeatureId)) = newModelDefinition

    Then("should create model instance")

    val createModelInstance = modelInstances.add(
      modelDefinitionId,
      name = Some(s"instance=${now.toEpochMilli}"),
      createdBy = "ModelInstanceCatalogSpec",
      createdAt = now,
      comment = None
    )

    val topColumns = Seq(
      CategoricalColumn.CategoricalValue(2, "banner", ModelMatrixEncoding.encode("banner"), 100, 100),
      CategoricalColumn.CategoricalValue(3, "mobile", ModelMatrixEncoding.encode("mobile"), 200, 300),
      CategoricalColumn.AllOther(4, 100, 400)
    )

    val indexColumns = Seq(
      CategoricalColumn.CategoricalValue(5, "google", ModelMatrixEncoding.encode("google"), 100, 100),
      CategoricalColumn.CategoricalValue(6, "yahoo", ModelMatrixEncoding.encode("yahoo"), 200, 300),
      CategoricalColumn.AllOther(7, 100, 400)
    )

    val binsColumns = Seq(
      BinColumn.LowerBin(8, 0.8, 100, 300),
      BinColumn.BinValue(9, 0.8, 2.4, 100, 300),
      BinColumn.UpperBin(10, 2.4, 100, 300)
    )

    val insert = for {
      id <- createModelInstance
      _ <- modelInstanceFeatures.addIdentityFeature(id, identityFeatureId, IntegerType, 1)
      _ <- modelInstanceFeatures.addTopFeature(id, topFeatureId, StringType, topColumns)
      _ <- modelInstanceFeatures.addIndexFeature(id, indexFeatureId, StringType, indexColumns)
      _ <- modelInstanceFeatures.addBinsFeature(id, binsFeatureId, DoubleType, binsColumns)
    } yield id

    val modelInstanceId = await(db.run(insert))

    And("find model instance in database")
    val instanceO = await(db.run(modelInstances.all)).find(_.id == modelInstanceId)
    assert(instanceO.isDefined)

    val instance = instanceO.get
    assert(instance.createdAt == now)
    assert(instance.features == 4)
    assert(instance.columns == 10)

    And("find model instance by id")
    val foundById = await(db.run(modelInstances.findById(modelInstanceId)))
    assert(foundById == instanceO)

    And("find model instance by name")
    val foundByName = await(db.run(modelInstances.list(name = Some(s"instance=${now.toEpochMilli}")))).headOption
    assert(foundByName == instanceO)

    And("find model instance by definition id")
    val foundByDefinition = await(db.run(modelInstances.list(definitionId = Some(modelDefinitionId)))).headOption
    assert(foundByDefinition == instanceO)

    And("read model instance columns")
    val features = await(db.run(modelInstanceFeatures.features(modelInstanceId)))
    val featureMap = features.map(f => f.feature.feature -> f).toMap
    assert(features.size == 4)

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

    And("get valid bins columns")
    assert(featureMap("ad_performance").isInstanceOf[ModelInstanceBinsFeature])
    assert(featureMap("ad_performance").feature == bins)
    assert(featureMap("ad_performance").extractType == DoubleType)
    assert(featureMap("ad_performance").asInstanceOf[ModelInstanceBinsFeature].columns == binsColumns)

  }

  it should "fail to add model feature with incorrect type" in {
    val (modelDefinitionId, Seq(_, topFeatureId, _, _)) = newModelDefinition

    val createModelInstance = modelInstances.add(
      modelDefinitionId,
      name = Some(s"instance=${now.toEpochMilli}"),
      createdBy = "ModelInstanceCatalogSpec",
      createdAt = now,
      comment = None
    )

    val insert = for {
      id <- createModelInstance
      _ <- modelInstanceFeatures.addIdentityFeature(id, topFeatureId, IntegerType, 1)
    } yield id

    intercept[IllegalArgumentException] {
      await(db.run(insert))
    }
  }

}
