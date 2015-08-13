package com.collective.modelmatrix.catalog

import java.security.MessageDigest
import java.time.Instant

import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz.{Tag, @@}

case class ModelDefinition(
  id: Int,
  name: Option[String],
  checksum: String,
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

  // TODO: place this outside so that we have one implementation
  private def md5(s: String): String = {
    MessageDigest.getInstance("MD5").digest(s.getBytes).map("%02X".format(_)).mkString
  }

  private def q(definitions: catalog.modelDefinitionsT): DBIO[Seq[ModelDefinition]] = {
    val grouped = (for {
      m <- definitions
      f <- featureDefinitions if m.id === f.modelDefinitionId
    } yield (m, f)).groupBy(t => t._1.*)

    val counted = grouped.map { case (model, features) =>
      (model._1, model._2, model._3, model._4, model._5, model._6, model._7, features.length)
    }

    counted.result.map(_.map(ModelDefinition.tupled)).map(_.sortBy(_.id))
  }

  def all: DBIO[Seq[ModelDefinition]] = {
    log.trace(s"Get all model definitions")
    q(modelDefinitions)
  }

  def findById(id: Int): DBIO[Option[ModelDefinition]] = {
    q(modelDefinitions.filter(_.id === id)).map(_.headOption)
  }

  /**
   * Find a specific ModelDefinition using the source hash
   */
  def findByChecksum(checksum: String): DBIO[Option[ModelDefinition]] = {
    q(modelDefinitions.filter(_.checksum === checksum)).map(_.headOption)
  }

  def list(name: Option[String] = None): DBIO[Seq[ModelDefinition]] = {
    var m: catalog.modelDefinitionsT = modelDefinitions
    name.foreach(n => m = m.filter(_.name like s"%$n%"))
    q(m)
  }

  def add(
    name: Option[String],
    source: String,
    createdBy: String,
    createdAt: Instant,
    comment: Option[String]
    ): DBIO[Int] = {

    log.trace(s"Add model definition. " +
      s"Created by: $createdBy @ $createdAt. " +
      s"Comment: ${comment.getOrElse("")}")

    val checksum = md5(source)

    modelDefinitions.filter(_.checksum === checksum).result.headOption.flatMap  {
      case Some(modelDefinition) =>
        log.info(s"ModelDefinition already exists. Id is ${modelDefinition._1}")
        DBIO.successful(modelDefinition._1)
      case None =>
        log.info(s"ModelDefinition doesn't exists. Creating it...")
        (modelDefinitions returning modelDefinitions.map(_.id)) +=
          ((AutoIncId, name, md5(source), source, createdBy, createdAt, comment))
    }.transactionally
  }

}
