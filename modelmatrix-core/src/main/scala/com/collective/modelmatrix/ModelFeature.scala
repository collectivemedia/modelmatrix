package com.collective.modelmatrix

import com.collective.modelmatrix.transform.Transform
import com.typesafe.config.Config

import scala.util.{Failure, Success, Try}
import scalaz.ValidationNel
import scalaz.syntax.apply._
import scalaz.syntax.validation._

case class ModelFeature(
  active: Boolean,
  group: String,
  feature: String,
  extract: String,
  transform: Transform
)

object ModelFeature {

  def parse(feature: String, config: Config): ValidationNel[String, ModelFeature] = {

    def string(p: String) = parameter(p)(_.getString)

    def boolean(p: String) = parameter(p)(_.getBoolean)

    def transform(p: String): ValidationNel[String, Transform] =
      string(p).fold(_.failure, {
        case "identity"  => Transform.identity(feature, config)
        case "top"       => Transform.top(feature, config)
        case "index"     => Transform.index(feature, config)
        case unknown     => s"Unknown transform type: $unknown".failureNel
      })

    def parameter[P](p: String)(f: Config => String => P): ValidationNel[String, P] =
      Try(f(config)(p)) match {
        case Success(s) =>
          s.successNel
        case Failure(err) =>
          s"Failed to load feature '$feature'. Error: ${err.getMessage}".failureNel
      }

    (
      boolean("active")      |@|
      string("group")        |@|
      feature.successNel     |@|
      string("extract")      |@|
      transform("transform")
    )(ModelFeature.apply)
  }

}
