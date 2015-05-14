package com.collective.modelmatrix.catalog

import java.time.Instant

import slick.driver.JdbcDriver

import scala.concurrent.ExecutionContext

case class ModelDefinition(
  id: Int,
  source: String,
  createdBy: String,
  createdAt: Instant,
  comment: String
)

class ModelDefinitions(val driver: JdbcDriver)(implicit val ec: ExecutionContext) {

  import driver.api._

  // scalastyle:off
  private class mmc_definition(tag: Tag) extends Table[(Int, String, String, Instant, String)](tag, "mmc_definition") {

    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def source = column[String]("source")
    def createdBy = column[String]("created_by")
    def createdAt = column[Instant]("created_at")
    def comment = column[String]("comment", O.Default(""))

    def * = (id, source, createdBy, createdAt, comment)
  }
  // scalastyle:on

  private val definitions = TableQuery[mmc_definition]

  def createSchema: DBIO[Unit] =
    definitions.schema.create

  def all: DBIO[Seq[ModelDefinition]] = {
    definitions.result.map(_.map(ModelDefinition.tupled))
  }

  def add(comment: Option[String]): DBIO[Int] = {
    (definitions returning definitions.map(_.id)) += ((-1, "source", "ebaka", Instant.now(), comment.getOrElse("")))
  }

}
