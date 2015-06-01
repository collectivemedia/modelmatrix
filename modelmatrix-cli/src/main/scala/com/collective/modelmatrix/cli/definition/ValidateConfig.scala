package com.collective.modelmatrix.cli.definition

import java.nio.file.Path

import com.collective.modelmatrix.cli.{ModelConfigurationParser, Script, _}
import com.typesafe.config.{ConfigFactory, ConfigResolveOptions}
import org.slf4j.LoggerFactory

import scalaz._

case class ValidateConfig(
  config: Path, configPath: String
) extends Script {

  private val log = LoggerFactory.getLogger(classOf[ValidateConfig])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  private lazy val parser = new ModelConfigurationParser(
    ConfigFactory.parseFile(config.toFile)
      .resolve(ConfigResolveOptions.defaults()),
    configPath
  )

  private implicit val failedFeatureFormat: ASCIITableFormat[(String, NonEmptyList[String])] =
    ASCIITableFormat[(String, NonEmptyList[String])](
      "Feature", "Errors".dataLeftAligned
    ) { obj =>
      val (feature, errors) = obj
      Array(feature, errors.list.mkString(System.lineSeparator()))
    }

  def run(): Unit = {
    log.info(s"Validate Model Matrix config: $configPath @ $config")

    val (failed, success) = parser.features().partition(_._2.isFailure)

    if (failed.nonEmpty) {
      Console.out.println(s"Incorrect configured model features: ${failed.size}")
      val failedCols = failed collect { case (feature, Failure(errors)) => (feature, errors) }
      failedCols.printASCIITable()
    }

    if (success.nonEmpty) {
      Console.out.println(s"Correct configured model features: ${failed.size}")
      val modelFeatures = success collect { case (_, Success(feature)) => feature }
      modelFeatures.printASCIITable()
    }
  }
}
