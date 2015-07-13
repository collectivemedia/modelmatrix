package com.collective.modelmatrix.catalog

import com.collective.modelmatrix.CategoricalColumn.{AllOther, CategoricalValue}
import com.collective.modelmatrix.{CategoricalColumn, BinColumn, ModelFeature}
import com.collective.modelmatrix.transform._
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
  columns: Seq[CategoricalColumn]
) extends ModelInstanceFeature {
  assert(feature.transform.isInstanceOf[Top],
    s"Wrong model feature transform function: $feature. Expected 'top'")
}

case class ModelInstanceIndexFeature(
  id: Int,
  modelInstanceId: Int,
  feature: ModelFeature,
  extractType: DataType,
  columns: Seq[CategoricalColumn]
) extends ModelInstanceFeature {
  assert(feature.transform.isInstanceOf[Index],
    s"Wrong model feature transform function: $feature. Expected 'index'")
}

case class ModelInstanceBinsFeature(
  id: Int,
  modelInstanceId: Int,
  feature: ModelFeature,
  extractType: DataType,
  columns: Seq[BinColumn]
) extends ModelInstanceFeature {
  assert(feature.transform.isInstanceOf[Bins],
    s"Wrong model feature transform function: $feature. Expected 'bins'")
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
      bins <- binsFeatures(modelInstanceId)
    } yield id ++ top ++ idx ++ bins

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
    columns: Seq[CategoricalColumn]
  ): DBIO[Int] = {

    for {
      transform <- featureDefinitionTransformType(featureDefinitionId)
      _ = require(transform == Transform.nameOf[Top], s"Wrong feature definition transform type: $transform. Expected: top")
      name <- featureDefinitionName(featureDefinitionId)
      _ = log.trace(s"Add top feature: $name. Columns: ${columns.size}")
      featureInstanceId <- (featureInstances returning featureInstances.map(_.id)) +=
        ((AutoIncId, modelInstanceId, featureDefinitionId, extractType))
      _ <- DBIO.sequence(columns.map {
        case CategoricalValue(columnId, sourceName, sourceValue, count, cumCount) =>
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
    columns: Seq[CategoricalColumn]
  ): DBIO[Int] = {

    for {
      transform <- featureDefinitionTransformType(featureDefinitionId)
      _ = require(transform == Transform.nameOf[Index], s"Wrong feature definition transform type: $transform. Expected: index")
      name <- featureDefinitionName(featureDefinitionId)
      _ = log.trace(s"Add index feature: $name. Columns: ${columns.size}")
      featureInstanceId <- (featureInstances returning featureInstances.map(_.id)) +=
        ((AutoIncId, modelInstanceId, featureDefinitionId, extractType))
      _ <- DBIO.sequence(columns.map {
        case CategoricalValue(columnId, sourceName, sourceValue, count, cumCount) =>
          indexColumns +=(AutoIncId, featureInstanceId, columnId, Some(sourceName), Some(sourceValue), count, cumCount)
        case AllOther(columnId, count, cumCount) =>
          indexColumns +=(AutoIncId, featureInstanceId, columnId, Option.empty[String], Option.empty[ByteVector], count, cumCount)
      })
    } yield featureInstanceId
  }

  def addBinsFeature(
    modelInstanceId: Int,
    featureDefinitionId: Int,
    extractType: DataType,
    columns: Seq[BinColumn]
  ): DBIO[Int] = {

    for {
      transform <- featureDefinitionTransformType(featureDefinitionId)
      _ = require(transform == Transform.nameOf[Bins], s"Wrong feature definition transform type: $transform. Expected: bins")
      name <- featureDefinitionName(featureDefinitionId)
      _ = log.trace(s"Add bins feature: $name. Columns: ${columns.size}")
      featureInstanceId <- (featureInstances returning featureInstances.map(_.id)) +=
        ((AutoIncId, modelInstanceId, featureDefinitionId, extractType))
      _ <- DBIO.sequence(columns.map {
        case BinColumn.LowerBin(columnId, high, count, sampleSize) =>
          binsColumns +=(AutoIncId, featureInstanceId, columnId, None, Some(high), count, sampleSize)
        case BinColumn.UpperBin(columnId, low, count, sampleSize) =>
          binsColumns +=(AutoIncId, featureInstanceId, columnId, Some(low), None, count, sampleSize)
        case BinColumn.BinValue(columnId, low, high, count, sampleSize) =>
          binsColumns +=(AutoIncId, featureInstanceId, columnId, Some(low), Some(high), count, sampleSize)
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

  private type CategoricalColumnRecord = (Int, Int, Int, Option[String], Option[ByteVector], Long, Long)

  private def toCategoricalColumn: CategoricalColumnRecord => CategoricalColumn = {
    case (_, _, columnId, Some(sourceName), Some(sourceValue), count, cumCount) =>
      CategoricalValue(columnId, sourceName, sourceValue, count, cumCount)

    case (_, _, columnId, None, None, count, cumCount) =>
      AllOther(columnId, count, cumCount)

    case (_, _, _, sourceName, sourceValue, _, _) =>
      sys.error(s"Wrong source name and value pair. Name: $sourceName. Value: $sourceValue")
  }

  private type BinsColumnRecord = (Int, Int, Int, Option[Double], Option[Double], Long, Long)

  private def toBinColumn: BinsColumnRecord => BinColumn = {
    case (_, _, columnId, None, Some(high), count, sampleSize) =>
      BinColumn.LowerBin(columnId, high, count, sampleSize)
    case (_, _, columnId, Some(low), None, count, sampleSize) =>
      BinColumn.UpperBin(columnId, low, count, sampleSize)
    case (_, _, columnId, Some(low), Some(high), count, sampleSize) =>
      BinColumn.BinValue(columnId, low, high, count, sampleSize)
    case error =>
      sys.error(s"Usupported bins columns record: $error")
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
        val columns = topColumns.filter(_.featureInstanceId === featureInstanceId)
          .result.map(_.map(toCategoricalColumn).sortBy(_.columnId))
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
        val columns = indexColumns.filter(_.featureInstanceId === featureInstanceId)
          .result.map(_.map(toCategoricalColumn).sortBy(_.columnId))
        columns.map(ModelInstanceIndexFeature(featureInstanceId, modelInstanceId, modelFeature, extractTime, _))
      })
    }
  }

  private def binsFeatures(modelInstanceId: Int): DBIO[Seq[ModelInstanceBinsFeature]] = {
    val features = for {
      fi <- featureInstances.filter(_.modelInstanceId === modelInstanceId)
      fd <- fi.featureDefinition
      if fd.transform === Transform.nameOf[Bins]
      fp <- binsParameters if fp.featureDefinitionId === fd.id
    } yield (fi.id, fd.active, fd.group, fd.feature, fd.extract, fp.nbins, fp.min_points, fp.min_pct, fi.extractType)

    features.result.flatMap { features =>
      DBIO.sequence(features.map { case (ftrInstanceId, active, group, feature, extract, nbins, minPts, minPct, extractTime) =>
        val modelFeature = ModelFeature(active, group, feature, extract, Bins(nbins, minPts, minPct))
        val columns = binsColumns.filter(_.featureInstanceId === ftrInstanceId)
          .result.map(_.map(toBinColumn).sortBy(_.columnId))
        columns.map(ModelInstanceBinsFeature(ftrInstanceId, modelInstanceId, modelFeature, extractTime, _))
      })
    }
  }

}
