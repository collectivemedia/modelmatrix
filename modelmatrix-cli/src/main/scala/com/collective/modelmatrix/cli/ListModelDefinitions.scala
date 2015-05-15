package com.collective.modelmatrix.cli

import com.bethecoder.ascii_table.ASCIITable
import com.collective.modelmatrix.catalog.{ModelMatrixCatalog, Schema, ModelDefinitions}
import com.typesafe.config.Config
import slick.driver.PostgresDriver
import slick.driver.PostgresDriver.api._

import scala.concurrent.ExecutionContext
import scalaz.@@

case class ListModelDefinitions(
  dbName: String,
  dbConfig: Config
)(implicit ec: ExecutionContext @@ ModelMatrixCatalog) extends Script {

  private lazy val db = Database.forConfig(dbName, dbConfig)
  private lazy val schema = new Schema(PostgresDriver)
  private lazy val modelDefinitions = new ModelDefinitions(schema)

  private val header: Array[String] = Array(
    "Id", "Name", "Created By", "Created At", "Comment"
  )

  def run(): Unit = {
    val definitions = blockOn(db.run(modelDefinitions.all))
    val data = definitions.map { definition =>
      Array(
        definition.id.toString,
        definition.name.getOrElse("n/a"),
        definition.createdBy,
        definition.createdAt.toString,
        definition.comment.getOrElse("n/a")
      )
    }

    Console.out.print(System.lineSeparator())
    ASCIITable.getInstance().printTable(header, data.toArray)
  }
}
