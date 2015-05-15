package com.collective.modelmatrix.catalog

import java.time.Instant

import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz.{Tag, @@}

case class ModelDefinition(
  id: Int,
  source: String,
  createdBy: String,
  createdAt: Instant,
  comment: String
)

class ModelDefinitions(val schema: Schema)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) {
  private val log = LoggerFactory.getLogger(classOf[ModelDefinitions])

  import schema._
  import driver.api._

  private implicit val executionContext = Tag.unwrap(ec)

  def all: DBIO[Seq[ModelDefinition]] = {
    log.trace(s"Get all model definitions")

    modelDefinitions.result.map(_.map(ModelDefinition.tupled))
  }

  def add(source: String, createdBy: String, createdAt: Instant, comment: Option[String]): DBIO[Int] = {
    log.trace(s"Add model definition. Created by: $createdBy @ $createdAt. Comment: ${comment.getOrElse("n/a")}")

    (modelDefinitions returning modelDefinitions.map(_.id)) +=
      ((AutoIncId, source, createdBy, createdAt, comment.getOrElse("")))
  }

}
