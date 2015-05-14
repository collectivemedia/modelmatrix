package com.collective.modelmatrix.transform

import com.typesafe.config.Config

import scala.util.{Failure, Success, Try}
import scalaz._

sealed trait Transform

object Transform {

  /**
   * Absence of transformation
   */
  case object Identity extends Transform

  /**
   * For distinct values of the column, find top values
   * by a quantity that cumulatively cover a given percentage
   * of this quantity. For example, find the top DMAs that
   * represent 99% of cookies, or find top sites that
   * are responsible for 90% of impressions.
   *
   * @param percentage cumulative cover percentage
   * @param allOther   include feature for all other values
   */
  case class Top(percentage: Double, allOther: Boolean) extends Transform

  /**
   * For distinct values of the column, find the values
   * with at least the minimum support in the data set.
   * Support for a value is defined as the percentage of a
   * total quantity that have that value. For example,
   * find segments that appear for at least 1% of the cookies.
   *
   * @param percentage support percentage
   * @param allOther   include feature for all other values
   */
  case class Index(percentage: Double, allOther: Boolean) extends Transform

  // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
  // *  Parse Transform function from config                                   *
  // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

  import scalaz.syntax.validation._
  import scalaz.syntax.apply._

  def identity(feature: String, config: Config): ValidationNel[String, Identity.type] = {
    Identity.successNel[String]
  }

  def top(feature: String, config: Config): ValidationNel[String, Top] = {
    new TopParser(feature).parse(config)
  }

  def index(feature: String, config: Config): ValidationNel[String, Index] = {
    new IndexParser(feature).parse(config)
  }

  private abstract class Parser[T <: Transform](transform: String, feature: String) {

    def parse(config: Config): ValidationNel[String, T]

    protected def double(p: String)(implicit cfg: Config) =
      parameter(cfg, p)(_.getDouble)

    protected def boolean(p: String)(implicit cfg: Config) =
      parameter(cfg, p)(_.getBoolean)

    private def parameter[P](config: Config, p: String)(f: Config => String => P): ValidationNel[String, P] = {
      Try(f(config)(p)) match {
        case Success(s) => s.successNel
        case Failure(err) =>
          s"Feature: $feature. Transform: $transform. Failed to load parameter: $p. Error: ${err.getMessage}".failureNel
      }
    }
  }

  private class TopParser(feature: String) extends Parser[Top]("top", feature) {
    def parse(config: Config): ValidationNel[String, Top] = {
      implicit val cfg = config
      (double("percentage") |@| boolean("allOther"))(Top.apply)
    }
  }

  private class IndexParser(feature: String) extends Parser[Index]("index", feature) {
    def parse(config: Config): ValidationNel[String, Index] = {
      implicit val cfg = config
      (double("percentage") |@| boolean("allOther"))(Index.apply)
    }
  }

}
