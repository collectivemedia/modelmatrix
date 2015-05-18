package com.collective.modelmatrix.cli

import java.util.function.BiConsumer

import com.collective.modelmatrix.ModelFeature
import com.typesafe.config.{Config, ConfigValue}

import scalaz.ValidationNel

class ModelConfigurationParser(config: Config, path: String = "features") {

  type FeatureDefinition = (String, ValidationNel[String, ModelFeature])

  def features(): Seq[FeatureDefinition] = {
    val builder = collection.mutable.ListBuffer.empty[FeatureDefinition]

    config.getObject(path).forEach(new BiConsumer[String, ConfigValue] {
      def accept(t: String, u: ConfigValue): Unit = {
        val parsedFeature = ModelFeature.parse(t, u.atKey(t), t)
        builder += (t -> parsedFeature)
      }
    })

    builder.toSeq
  }
}
