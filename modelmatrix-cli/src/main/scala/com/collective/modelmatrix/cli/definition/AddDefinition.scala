package com.collective.modelmatrix.cli.definition

import java.nio.file.Path
import java.time.Instant

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{CliModelCatalog, ModelConfigurationParser, Script}
import com.typesafe.config.{Config, ConfigFactory, ConfigResolveOptions}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._

case class AddDefinition(
  config: Path,
  configPath: String,
  name: Option[String],
  comment: Option[String]
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[AddDefinition])

  private implicit val unwrap = Tag.unwrap(ec)

  private val parser = new ModelConfigurationParser(
    ConfigFactory.parseFile(config.toFile).resolve(ConfigResolveOptions.defaults()),
    configPath
  )

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"Add Model Matrix definition. " +
      s"Config: $configPath @ $config. " +
      s"Name: $name. " +
      s"Comment: $comment")

    val features = parser.features()

    val errors = features collect { case (f, Failure(e)) => (f, e) }
    val success = features collect { case (_, Success(feature)) => feature }

    if (errors.nonEmpty) {
      Console.out.println(s"Incorrect configured model features: ${errors.size}")
      errors.printASCIITable()
    }

    if (success.nonEmpty && errors.isEmpty) {
      val addModelDefinition = modelDefinitions.add(
        name = name,
        source = definitionSource(),
        createdBy = System.getProperty("user.name"),
        createdAt = Instant.now(),
        comment = comment
      )

      val insert = for {
        id <- addModelDefinition
        featureId <- modelDefinitionFeatures.addFeatures(id, success: _*)
      } yield (id, featureId)

      import driver.api._
      val (modelDefinitionId, _) = blockOn(db.run(insert.transactionally))

      Console.out.println(s"Successfully created new model definition")
      Console.out.println(s"Matrix Model definition id: $modelDefinitionId")
    }
  }

  private def definitionSource(): String = {
    scala.io.Source.fromFile(config.toFile).getLines().mkString(System.lineSeparator())
  }

}
