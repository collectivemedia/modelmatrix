package com.collective.modelmatrix.cli.definition

import java.nio.file.Path

import com.collective.modelmatrix.cli.{ModelConfigurationParser, Script}
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
    ConfigFactory.parseFile(config.toFile).resolve(ConfigResolveOptions.defaults()),
    configPath
  )

  def run(): Unit = {
    log.info(s"Validate Model Matrix config: $configPath @ $config")

    val features = parser.features()

    val errors = features collect { case (f, Failure(e)) => (f, e) }
    val success = features collect { case (_, Success(feature)) => feature }

    if (errors.nonEmpty) {
      Console.out.println(s"Incorrect configured model features: ${errors.size}")
      errors.printASCIITable()
    }

    if (success.nonEmpty) {
      Console.out.println(s"Correct configured model features: ${success.size}")
      success.printASCIITable()
    }
  }
}
