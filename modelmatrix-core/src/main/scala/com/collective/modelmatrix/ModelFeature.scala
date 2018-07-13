package com.collective.modelmatrix

import com.collective.modelmatrix.transform.Transform
import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf

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

  // Validate extract expressions using SqlParser that used in DataFrame.selectExpr
  private val conf = new SQLConf()
  private val sqlParser = new CatalystSqlParser(conf)

  def parse(feature: String, config: Config, path: String): ValidationNel[String, ModelFeature] = {
    parse(feature, config.getConfig(path))
  }

  def parse(feature: String, config: Config): ValidationNel[String, ModelFeature] = {

    def string(p: String) = parameter(p)(_.getString)

    def expression(p: String) = {
      import scalaz.Validation.FlatMap._
      string(p).flatMap { input =>
        Try(sqlParser.parseExpression(input)) match {
          case Success(parsed) => input.successNel
          case Failure(err)    => s"Failed to parse extract expression: $err".failureNel
        }
      }
    }

    def boolean(p: String) = parameter(p)(_.getBoolean)

    def transform(p: String): ValidationNel[String, Transform] =
      string(p).fold(_.failure, {
        case "identity"  => Transform.identity(config)
        case "top"       => Transform.top(config)
        case "index"     => Transform.index(config)
        case "bins"      => Transform.bins(config)
        case unknown     => s"Unknown transform type: $unknown".failureNel
      })

    def parameter[P](p: String)(f: Config => String => P): ValidationNel[String, P] =
      Try(f(config)(p)) match {
        case Success(s) =>
          s.successNel
        case Failure(err) =>
          s"Can't parse parameter '$p'. Error: ${err.getMessage}".failureNel
      }

    (
      boolean("active")      |@|
      string("group")        |@|
      feature.successNel     |@|
      expression("extract")  |@|
      transform("transform")
    )(ModelFeature.apply _)
  }

}
