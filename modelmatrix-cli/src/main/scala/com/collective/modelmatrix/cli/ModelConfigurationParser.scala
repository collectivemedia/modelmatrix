package com.collective.modelmatrix.cli

import java.nio.charset.CodingErrorAction
import java.util.function.BiConsumer

import com.collective.modelmatrix.ModelFeature
import com.typesafe.config.{Config, ConfigValue}

import scala.io.Codec
import scalaz.{Success, Failure, ValidationNel}

class ModelConfigurationParser(config: Config, path: String = "features") {

  type FeatureDefinition = (String, ValidationNel[String, ModelFeature])

  private val configLines: Seq[(String, Int)] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val originURL = config.origin().url()
    val lines =
      if (originURL != null) {
      scala.io.Source.fromURL(originURL).getLines().toSeq
    } else Seq.empty

    lines.zipWithIndex
  }

  // Try to find feature row index in original cofig if possible
  private def featureIndex(f: String): Int = {
    configLines.find(_._1.contains(f)).map(_._2).getOrElse(0)
  }

  def features(): Seq[FeatureDefinition] = {
    val builder = collection.mutable.ListBuffer.empty[FeatureDefinition]

    config.getObject(path).forEach(new BiConsumer[String, ConfigValue] {
      def accept(t: String, u: ConfigValue): Unit = {
        val parsedFeature = ModelFeature.parse(t, u.atKey(t), t)
        builder += (t -> parsedFeature)
      }
    })

    builder.toSeq.sortBy {
      case (f, Success(feature)) => (true, featureIndex(feature.feature), feature.group, feature.feature)
      case (f, Failure(_)) => (false, featureIndex(f), "", f)
    }
  }
}
