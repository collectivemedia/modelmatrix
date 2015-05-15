package com.collective.modelmatrix.cli.definition

import java.time.ZoneId
import java.time.format.{DateTimeFormatter, FormatStyle}
import java.util.Locale

import com.bethecoder.ascii_table.ASCIITable
import com.collective.modelmatrix.catalog.{ModelDefinition, ModelDefinitions, ModelMatrixCatalog, Schema}
import com.collective.modelmatrix.cli.Script
import com.typesafe.config.Config
import slick.driver.PostgresDriver
import slick.driver.PostgresDriver.api._

import scala.concurrent.ExecutionContext
import scalaz._

trait ListDefinitions extends Script {

  def dbName: String
  def dbConfig: Config
  implicit def ec: ExecutionContext @@ ModelMatrixCatalog

  protected lazy val db = Database.forConfig(dbName, dbConfig)
  protected lazy val schema = new Schema(PostgresDriver)
  protected lazy val modelDefinitions = new ModelDefinitions(schema)

  private val formatter =
    DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
      .withLocale(Locale.US)
      .withZone(ZoneId.systemDefault())

  protected def printDefinitions(definitions: Seq[ModelDefinition]): Unit = {
    val header: Array[String] = Array(
      "Id", "Name", "Created By", "Created At", "Comment", "Features"
    )

    val noData: Array[Array[String]] = Array(Array.fill(header.length)("--"))

    val result = definitions.map { definition =>
      Array(
        definition.id.toString,
        definition.name.getOrElse("n/a"),
        definition.createdBy,
        formatter.format(definition.createdAt),
        definition.comment.getOrElse("n/a"),
        definition.features.toString
      )
    }

    Console.out.print(System.lineSeparator() + System.lineSeparator() + System.lineSeparator())
    Console.out.println(s"Total number of Model Matrix definitions: ${definitions.length}")
    ASCIITable.getInstance().printTable(header, if (result.nonEmpty) result.toArray else noData)
  }

}
