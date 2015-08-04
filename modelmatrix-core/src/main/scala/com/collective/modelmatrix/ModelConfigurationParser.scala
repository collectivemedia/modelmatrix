package com.collective.modelmatrix

import java.nio.charset.CodingErrorAction
import java.security.MessageDigest
import java.util.function.BiConsumer

import com.typesafe.config.{Config, ConfigValue}

import scala.io.Codec
import scalaz.{Failure, Success, ValidationNel}

class ModelConfigurationParser(config: Config, path: String = "features") {

  type FeatureDefinition = (String, ValidationNel[String, ModelFeature])

  private lazy val configLines: Seq[(String, Int)] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    contentLines.zipWithIndex
  }

  // Try to find feature row index in original config if possible
  private def featureIndex(f: String): Int = {
    configLines.find(_._1.contains(f)).map(_._2).getOrElse(0)
  }

  private[this] val originUrl = config.origin().url()

  // configuration file as lines
  lazy val contentLines: Seq[String] = {
    if (originUrl != null) {
      scala.io.Source.fromURL(originUrl).getLines().toSeq
      // ideally this case below should never happen unless the Config passed in argument is not parsed from a file
    } else Seq.empty
  }

  // configuration file as a String
  lazy val content: String = contentLines.mkString(System.lineSeparator())

  // md5sum of the configuration content
  lazy val checksum: String = MessageDigest.getInstance("MD5").digest(content.getBytes).map("%02X".format(_)).mkString

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
