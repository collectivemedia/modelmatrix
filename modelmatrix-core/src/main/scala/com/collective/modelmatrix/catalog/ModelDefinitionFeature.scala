package com.collective.modelmatrix.catalog

import com.collective.modelmatrix.ModelFeature
import com.collective.modelmatrix.transform.{Index, Top, Identity, Transform}
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scalaz.{Tag, @@}

case class ModelDefinitionFeature(
  id: Int,
  modelDefinitionId: Int,
  feature: ModelFeature
)

class ModelDefinitionFeatures(val catalog: ModelMatrixCatalog)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) {
  private val log = LoggerFactory.getLogger(classOf[ModelDefinitionFeatures])

  import catalog.tables._
  import catalog.driver.api._

  private implicit val executionContext = Tag.unwrap(ec)

  def features(modelDefinitionId: Int): DBIO[Seq[ModelDefinitionFeature]] = {
    log.trace(s"Get model definition features. Model definition id: $modelDefinitionId")

    val features = for {
      id <- identityFeatures(modelDefinitionId)
      top <- topFeatures(modelDefinitionId)
      idx <- indexFeatures(modelDefinitionId)
    } yield id ++ top ++ idx

    features.map(_.sortBy(_.id))
  }
  
  def addFeatures(modelDefinitionId: Int, features: ModelFeature*): DBIO[Seq[Int]] = {
    log.trace(s"Add ${features.length} featured to model definition id: $modelDefinitionId")

    val inserts: Seq[DBIO[Int]] = features map {
      case ModelFeature(active, group, feature, extract, id@Identity) =>
        (featureDefinitions returning featureDefinitions.map(_.id)) +=
          ((AutoIncId, modelDefinitionId, active, group, feature, extract, id.stringify))

      case ModelFeature(active, group, feature, extract, top: Top) =>
        for {
          featureId <- (featureDefinitions returning featureDefinitions.map(_.id)) +=
            ((AutoIncId, modelDefinitionId, active, group, feature, extract, top.stringify))
          _ <- topParameters += (AutoIncId, featureId, top.cover, top.allOther)
        } yield featureId

      case ModelFeature(active, group, feature, extract, index: Index) =>
        for {
          featureId <- (featureDefinitions returning featureDefinitions.map(_.id)) +=
            ((AutoIncId, modelDefinitionId, active, group, feature, extract, index.stringify))
          _ <- indexParameters += (AutoIncId, featureId, index.support, index.allOther)
        } yield featureId
    }

    DBIO.sequence(inserts)
  }

  private def identityFeatures(modelDefinitionId: Int): DBIO[Seq[ModelDefinitionFeature]] = {
    type Out = (Int, Int, Boolean, String, String, String, String)

    val extract: Out => ModelDefinitionFeature = {
      case (id, modelDefId, active, group, feature, ex, transform) =>
        ModelDefinitionFeature(id, modelDefId, ModelFeature(active, group, feature, ex, Identity))
    }

    featureDefinitions
      .filter(_.modelDefinitionId === modelDefinitionId)
      .filter(_.transform === Transform.nameOf[Identity.type])
      .map(f => (f.id, f.modelDefinitionId, f.active, f.group, f.feature, f.extract, f.transform))
      .result.map(_.map(extract))
  }

  private def topFeatures(modelDefinitionId: Int): DBIO[Seq[ModelDefinitionFeature]] = {
    type Out = (Int, Int, Boolean, String, String, String, String, Double, Boolean)

    val extract: Out => ModelDefinitionFeature = {
      case (id, modelDefId, active, group, feature, ex, transform, p, allOther) =>
        ModelDefinitionFeature(id, modelDefId, ModelFeature(active, group, feature, ex, Top(p, allOther)))
    }

    val q = for {
      f <- featureDefinitions
        .filter(_.modelDefinitionId === modelDefinitionId)
        .filter(_.transform === Transform.nameOf[Top])
      p <- topParameters if f.id === p.featureDefinitionId
    } yield (f.id, f.modelDefinitionId, f.active, f.group, f.feature, f.extract, f.transform, p.cover, p.allOther)

    q.result.map(_.map(extract))
  }

  private def indexFeatures(modelDefinitionId: Int): DBIO[Seq[ModelDefinitionFeature]] = {
    type Out = (Int, Int, Boolean, String, String, String, String, Double, Boolean)

    val extract: Out => ModelDefinitionFeature = {
      case (id, modelDefId, active, group, feature, ex, transform, p, allOther) =>
        ModelDefinitionFeature(id, modelDefId, ModelFeature(active, group, feature, ex, Index(p, allOther)))
    }

    val q = for {
      f <- featureDefinitions
        .filter(_.modelDefinitionId === modelDefinitionId)
        .filter(_.transform === Transform.nameOf[Index])
      p <- indexParameters if f.id === p.featureDefinitionId
    } yield (f.id, f.modelDefinitionId, f.active, f.group, f.feature, f.extract, f.transform, p.support, p.allOther)

    q.result.map(_.map(extract))
  }

}
