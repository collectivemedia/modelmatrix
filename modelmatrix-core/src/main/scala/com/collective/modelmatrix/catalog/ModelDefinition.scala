package com.collective.modelmatrix.catalog

import java.time.Instant

import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz.{Tag, @@}

case class ModelDefinition(
  id: Int,
  name: Option[String],
  source: String,
  createdBy: String,
  createdAt: Instant,
  comment: Option[String],
  features: Int
)

class ModelDefinitions(val catalog: ModelMatrixCatalog)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) {
  private val log = LoggerFactory.getLogger(classOf[ModelDefinitions])

  import catalog.tables._
  import catalog.driver.api._

  private implicit val executionContext = Tag.unwrap(ec)

  private def q(definitions: catalog.modelDefinitionsT): DBIO[Seq[ModelDefinition]] = {
    val grouped = (for {
      m <- definitions
      f <- featureDefinitions if m.id === f.modelDefinitionId
    } yield (m, f)).groupBy(t => t._1.*)

    val counted = grouped.map { case (model, features) =>
      (model._1, model._2, model._3, model._4, model._5, model._6, features.length)
    }

    counted.result.map(_.map(ModelDefinition.tupled))
  }

  def all: DBIO[Seq[ModelDefinition]] = {
    log.trace(s"Get all model definitions")
    q(modelDefinitions)
  }

  def findById(id: Int): DBIO[Option[ModelDefinition]] = {
    q(modelDefinitions.filter(_.id === id)).map(_.headOption)
  }

  def findByName(name: String): DBIO[Seq[ModelDefinition]] = {
    q(modelDefinitions.filter(_.name like s"%$name%"))
  }

  def add(name: Option[String], source: String, createdBy: String, createdAt: Instant, comment: Option[String]): DBIO[Int] = {
    log.trace(s"Add model definition. Created by: $createdBy @ $createdAt. Comment: ${comment.getOrElse("n/a")}")

    (modelDefinitions returning modelDefinitions.map(_.id)) +=
      ((AutoIncId, name, source, createdBy, createdAt, comment))
  }

}
