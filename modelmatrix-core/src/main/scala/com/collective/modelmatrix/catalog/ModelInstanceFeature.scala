package com.collective.modelmatrix.catalog

import com.collective.modelmatrix.CategorialColumn.{AllOther, CategorialValue}
import com.collective.modelmatrix.{CategorialColumn, ModelFeature}
import com.collective.modelmatrix.transform.{Transform, Identity, Index, Top}
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory
import scodec.bits.ByteVector

import scala.concurrent.ExecutionContext
import scalaz.{@@, Tag}


sealed trait ModelInstanceFeature {
  def id: Int
  def modelInstanceId: Int
  def feature: ModelFeature
  def extractType: DataType
}

case class ModelInstanceIdentityFeature(
  id: Int,
  modelInstanceId: Int,
  feature: ModelFeature,
  extractType: DataType,
  columnId: Int
) extends ModelInstanceFeature {
  assert(feature.transform.isInstanceOf[Identity.type],
    s"Wrong model feature transform function: $feature. Expected 'identity'")
}

case class ModelInstanceTopFeature(
  id: Int,
  modelInstanceId: Int,
  feature: ModelFeature,
  extractType: DataType,
  columns: Seq[CategorialColumn]
) extends ModelInstanceFeature {
  assert(feature.transform.isInstanceOf[Top],
    s"Wrong model feature transform function: $feature. Expected 'top'")
}

case class ModelInstanceIndexFeature(
  id: Int,
  modelInstanceId: Int,
  feature: ModelFeature,
  extractType: DataType,
  columns: Seq[CategorialColumn]
) extends ModelInstanceFeature {
  assert(feature.transform.isInstanceOf[Index],
    s"Wrong model feature transform function: $feature. Expected 'index'")
}

class ModelInstanceFeatures(val catalog: ModelMatrixCatalog)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) {
  private val log = LoggerFactory.getLogger(classOf[ModelInstanceFeatures])

  import catalog.driver.api._
  import catalog.tables._

  private implicit val executionContext = Tag.unwrap(ec)

  def features(modelInstanceId: Int): DBIO[Seq[ModelInstanceFeature]] = {
    log.trace(s"Get model instance features. Model instance id: $modelInstanceId")

    val features = for {
      id <- identityFeatures(modelInstanceId)
      top <- topFeatures(modelInstanceId)
      idx <- indexFeatures(modelInstanceId)
    } yield id ++ top ++ idx

    features.map(_.sortBy(_.id))
  }

  def addIdentityFeature(
    modelInstanceId: Int,
    featureDefinitionId: Int,
    extractType: DataType,
    columnId: Int
  ): DBIO[Int] = {

    for {
      transform <- featureDefinitionTransformType(featureDefinitionId)
      _ = require(transform == Identity.stringify, s"Wrong feature definition transform type: $transform. Expected: identity")
      name <- featureDefinitionName(featureDefinitionId)
      _ = log.trace(s"Add identity feature: $name")
      featureInstanceId <- (featureInstances returning featureInstances.map(_.id)) +=
        ((AutoIncId, modelInstanceId, featureDefinitionId, extractType))
      _ <- identityColumns += (AutoIncId, featureInstanceId, columnId)
    } yield featureInstanceId
  }

  def addTopFeature(
    modelInstanceId: Int,
    featureDefinitionId: Int,
    extractType: DataType,
    columns: Seq[CategorialColumn]
  ): DBIO[Int] = {

    for {
      transform <- featureDefinitionTransformType(featureDefinitionId)
      _ = require(transform == Transform.nameOf[Top], s"Wrong feature definition transform type: $transform. Expected: top")
      name <- featureDefinitionName(featureDefinitionId)
      _ = log.trace(s"Add top feature: $name. Columns: ${columns.size}")
      featureInstanceId <- (featureInstances returning featureInstances.map(_.id)) +=
        ((AutoIncId, modelInstanceId, featureDefinitionId, extractType))
      _ <- DBIO.sequence(columns.map {
        case CategorialValue(columnId, sourceName, sourceValue, count, cumCount) =>
          topColumns +=(AutoIncId, featureInstanceId, columnId, Some(sourceName), Some(sourceValue), count, cumCount)
        case AllOther(columnId, count, cumCount) =>
          topColumns +=(AutoIncId, featureInstanceId, columnId, Option.empty[String], Option.empty[ByteVector], count, cumCount)
      })
    } yield featureInstanceId
  }

  def addIndexFeature(
    modelInstanceId: Int,
    featureDefinitionId: Int,
    extractType: DataType,
    columns: Seq[CategorialColumn]
  ): DBIO[Int] = {

    for {
      transform <- featureDefinitionTransformType(featureDefinitionId)
      _ = require(transform == Transform.nameOf[Index], s"Wrong feature definition transform type: $transform. Expected: index")
      name <- featureDefinitionName(featureDefinitionId)
      _ = log.trace(s"Add index feature: $name. Columns: ${columns.size}")
      featureInstanceId <- (featureInstances returning featureInstances.map(_.id)) +=
        ((AutoIncId, modelInstanceId, featureDefinitionId, extractType))
      _ <- DBIO.sequence(columns.map {
        case CategorialValue(columnId, sourceName, sourceValue, count, cumCount) =>
          indexColumns +=(AutoIncId, featureInstanceId, columnId, Some(sourceName), Some(sourceValue), count, cumCount)
        case AllOther(columnId, count, cumCount) =>
          indexColumns +=(AutoIncId, featureInstanceId, columnId, Option.empty[String], Option.empty[ByteVector], count, cumCount)
      })
    } yield featureInstanceId
  }

  private def featureDefinitionName(featureDefinitionId: Int): DBIO[String] = {
    featureDefinitions.filter(_.id === featureDefinitionId).map(_.feature).result.headOption.map {
      case None => sys.error(s"Can't find feature definition by id: $featureDefinitionId")
      case Some(s) => s
    }
  }

  private def featureDefinitionTransformType(featureDefinitionId: Int): DBIO[String] = {
    featureDefinitions.filter(_.id === featureDefinitionId).map(_.transform).result.headOption.map {
      case None => sys.error(s"Can't find feature definition by id: $featureDefinitionId")
      case Some(s) => s
    }
  }

  private def identityFeatures(modelInstanceId: Int): DBIO[Seq[ModelInstanceIdentityFeature]] = {
    type Out = (Int, Boolean, String, String, String, DataType, Int)

    val toFeature: Out => ModelInstanceIdentityFeature = {
      case (id, active, group, feature, extract, extractType, columnId) =>
        val modelFeature = ModelFeature(active, group, feature, extract, Identity)
        ModelInstanceIdentityFeature(id, modelInstanceId, modelFeature, extractType, columnId)
    }

    val q = for {
      fi <- featureInstances.filter(_.modelInstanceId === modelInstanceId)
      fd <- fi.featureDefinition
      if fd.transform === Transform.nameOf[Identity.type]
      col <- identityColumns if col.featureInstanceId === fi.id
    } yield (fi.id, fd.active, fd.group, fd.feature, fd.extract, fi.extractType, col.columnId)

    q.result.map(_.map(toFeature))
  }

  type CategorialColumnRecord = (Int, Int, Int, Option[String], Option[ByteVector], Long, Long)
  
  private def toCategorialColumn: CategorialColumnRecord => CategorialColumn = {
    case (_, _, columnId, Some(sourceName), Some(sourceValue), count, cumCount) =>
      CategorialValue(columnId, sourceName, sourceValue, count, cumCount)

    case (_, _, columnId, None, None, count, cumCount) =>
      AllOther(columnId, count, cumCount)

    case (_, _, _, sourceName, sourceValue, _, _) =>
      sys.error(s"Wrong source name and value pair. Name: $sourceName. Value: $sourceValue")
  }
  
  private def topFeatures(modelInstanceId: Int): DBIO[Seq[ModelInstanceTopFeature]] = {
    val features = for {
      fi <- featureInstances.filter(_.modelInstanceId === modelInstanceId)
      fd <- fi.featureDefinition
      if fd.transform === Transform.nameOf[Top]
      fp <- topParameters if fp.featureDefinitionId === fd.id
    } yield (fi.id, fd.active, fd.group, fd.feature, fd.extract, fp.cover, fp.allOther, fi.extractType)

    features.result.flatMap { features =>
      DBIO.sequence(features.map { case (featureInstanceId, active, group, feature, extract, cover, allOther, extractTime) =>
        val modelFeature = ModelFeature(active, group, feature, extract, Top(cover, allOther))
        val columns = topColumns.filter(_.featureInstanceId === featureInstanceId).result.map(_.map(toCategorialColumn))
        columns.map(ModelInstanceTopFeature(featureInstanceId, modelInstanceId, modelFeature, extractTime, _))
      })
    }
  }

  private def indexFeatures(modelInstanceId: Int): DBIO[Seq[ModelInstanceIndexFeature]] = {
    val features = for {
      fi <- featureInstances.filter(_.modelInstanceId === modelInstanceId)
      fd <- fi.featureDefinition
      if fd.transform === Transform.nameOf[Index]
      fp <- indexParameters if fp.featureDefinitionId === fd.id
    } yield (fi.id, fd.active, fd.group, fd.feature, fd.extract, fp.support, fp.allOther, fi.extractType)

    features.result.flatMap { features =>
      DBIO.sequence(features.map { case (featureInstanceId, active, group, feature, extract, support, allOther, extractTime) =>
        val modelFeature = ModelFeature(active, group, feature, extract, Index(support, allOther))
        val columns = indexColumns.filter(_.featureInstanceId === featureInstanceId).result.map(_.map(toCategorialColumn))
        columns.map(ModelInstanceIndexFeature(featureInstanceId, modelInstanceId, modelFeature, extractTime, _))
      })
    }
  }
}
