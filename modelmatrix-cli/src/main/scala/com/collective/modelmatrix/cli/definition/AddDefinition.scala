package com.collective.modelmatrix.cli.definition

import java.nio.file.Path
import java.time.Instant

import com.bethecoder.ascii_table.{ASCIITableHeader, ASCIITable}
import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{Script, ModelConfigurationParser, CliModelCatalog}
import com.collective.modelmatrix.cli._
import com.typesafe.config.{ConfigResolveOptions, ConfigFactory, Config}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._

case class AddDefinition(
  config: Path,
  configPath: String,
  name: Option[String],
  comment: Option[String],
  dbName: String,
  dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[AddDefinition])

  private implicit val unwrap = Tag.unwrap(ec)

  private lazy val parser = new ModelConfigurationParser(
    ConfigFactory.parseFile(config.toFile)
      .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true)),
    configPath
  )

  private def definitionSource(): String = {
    scala.io.Source.fromFile(config.toFile).getLines().mkString(System.lineSeparator())
  }

  def run(): Unit = {
    log.info(s"Add Model Matrix definition. " +
      s"Config: $configPath @ $config. " +
      s"Name: $name. " +
      s"Comment: $comment. " +
      s"Database: $dbName @ ${dbConfig.origin().filename()}")

    val (failed, success) = parser.features().partition(_._2.isFailure)

    // Fail in case of bad configuration
    if (failed.nonEmpty) {
      Console.out.println(s"Incorrect configured model features: ${failed.size}")

      val featuresHeader: Array[ASCIITableHeader] = Array(
        "Feature", "Error".dataLeftAligned
      )

      val failedCols = failed collect {
        case (feature, Failure(errors)) =>
          Array(feature, errors.list.mkString(System.lineSeparator()))
      }

      ASCIITable.getInstance().printTable(featuresHeader, failedCols.toArray)

      sys.error(s"Incorrect configured model features: ${failed.size}")
    }

    // Save definition to catalog in other case

    val addModelDefinition = modelDefinitions.add(
      name = name,
      source = definitionSource(),
      createdBy = System.getProperty("user.name"),
      createdAt = Instant.now(),
      comment = comment
    )

    val insert = for {
      id <- addModelDefinition
      featureId <- modelDefinitionFeatures.addFeatures(id, success.map(_._2.toOption.get):_*)
    } yield (id, featureId)

    val (modelDefinitionId, featuresId) = blockOn(db.run(insert))
    Console.out.println(s"Successfully created new model definition")
    Console.out.println(s"Matrix Model definition id: $modelDefinitionId")
    Console.out.println(s"Matrix Model features count: ${featuresId.length}")
  }
}
