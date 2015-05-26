package com.collective.modelmatrix.cli.definition

import java.nio.file.Path

import com.bethecoder.ascii_table.{ASCIITableHeader, ASCIITable}
import com.collective.modelmatrix.ModelFeature
import com.collective.modelmatrix.cli.{ModelConfigurationParser, Script}
import com.collective.modelmatrix.cli._
import com.typesafe.config.{ConfigResolveOptions, ConfigFactory}

import scalaz._

/**
 * Validate Matrix Model definition config
 */
case class Validate(
  config: Path, configPath: String
) extends Script {

  def run(): Unit = {

    val featuresHeader: Array[ASCIITableHeader] = Array(
      "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters"
    )

    val failedFeaturesHeader: Array[ASCIITableHeader] = Array(
      "Feature", "Errors".dataLeftAligned
    )

    val parser = new ModelConfigurationParser(
      ConfigFactory.parseFile(config.toFile)
        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true)),
      configPath
    )

    val (failed, success) = parser.features().partition(_._2.isFailure)

    if (failed.nonEmpty) {
      Console.out.println(s"Incorrect configured model features: ${failed.size}")

      val failedCols = failed collect {
        case (feature, Failure(errors)) =>
          Array(feature, errors.list.mkString(System.lineSeparator()))
      }

      ASCIITable.getInstance().printTable(failedFeaturesHeader, failedCols.toArray)
    }

    if (success.nonEmpty) {
      Console.out.println(s"Correct configured model features: ${failed.size}")

      val successCols = success collect {
        case (feature, Success(ModelFeature(a, g, f, e, t))) =>
          Array(
            a.toString,
            g,
            feature,
            e,
            t.stringify,
            printParameters(t)
          )
      }

      ASCIITable.getInstance().printTable(featuresHeader, successCols.toArray)
    }
  }
}
