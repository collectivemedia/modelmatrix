package com.collective.modelmatrix.catalog

import java.time.Instant

import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz.{Tag, @@}

case class ModelInstance(
  id: Int,
  modelDefinitionId: Int,
  name: Option[String],
  createdBy: String,
  createdAt: Instant,
  comment: Option[String],
  features: Int,
  columns: Int
)

class ModelInstances(val catalog: ModelMatrixCatalog)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) {
  private val log = LoggerFactory.getLogger(classOf[ModelInstances])

  import catalog.tables._
  import catalog.driver.api._

  private implicit val executionContext = Tag.unwrap(ec)

  private def q(instances: catalog.modelInstancesT): DBIO[Seq[ModelInstance]] = {

    val grouped = (for {
      m <- instances
      ((((f, idCol), topCol), indCol), binCol) <- featureInstances
        .joinLeft(identityColumns).on(_.id === _.featureInstanceId)
        .joinLeft(topColumns).on(_._1.id === _.featureInstanceId)
        .joinLeft(indexColumns).on(_._1._1.id === _.featureInstanceId)
        .joinLeft(binsColumns).on(_._1._1._1.id === _.featureInstanceId)
      if f.modelInstanceId === m.id
    } yield (m, f, idCol, topCol, indCol, binCol)).groupBy(t => t._1.*)

    val counted = grouped.map { case (model, group) =>

      val features = group.map(_._2).map(_.featureDefinitionId).countDistinct
      val identityColumns = group.map(_._3).length
      val topColumns = group.map(_._4).length
      val indexColumns = group.map(_._5).length
      val binsColumns = group.map(_._6).length

      val nColumns = identityColumns + topColumns + indexColumns + binsColumns
      (model._1, model._2, model._3, model._4, model._5, model._6, features, nColumns)
    }

    counted.result.map(_.map(ModelInstance.tupled)).map(_.sortBy(_.id))
  }

  def all: DBIO[Seq[ModelInstance]] = {
    log.trace(s"Get all model instances")
    q(modelInstances)
  }

  def findById(id: Int): DBIO[Option[ModelInstance]] = {
    q(modelInstances.filter(_.id === id)).map(_.headOption)
  }

  def list(definitionId: Option[Int] = None, name: Option[String] = None): DBIO[Seq[ModelInstance]] = {
    var m: catalog.modelInstancesT = modelInstances
    definitionId.foreach(id => m = m.filter(_.modelDefinitionId === id))
    name.foreach(n => m = m.filter(_.name like s"%$n%"))
    q(m)
  }

  def add(
    modelDefinitionId: Int,
    name: Option[String],
    createdBy: String,
    createdAt: Instant,
    comment: Option[String]
  ): DBIO[Int] = {

    log.trace(s"Add model instance of mode definition id: $modelDefinitionId. " +
      s"Created by: $createdBy @ $createdAt. " +
      s"Comment: ${comment.getOrElse("")}")

    (modelInstances returning modelInstances.map(_.id)) +=
      ((AutoIncId, modelDefinitionId, name, createdBy, createdAt, comment))
  }

}
