package com.collective.modelmatrix.transform

import com.typesafe.config.Config

import scala.util.{Failure, Success, Try}
import scalaz._

sealed trait Transform

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
 * @param cover      cumulative cover percentage
 * @param allOther   include feature for all other values
 */
case class Top(cover: Double, allOther: Boolean) extends Transform

/**
 * For distinct values of the column, find the values
 * with at least the minimum support in the data set.
 * Support for a value is defined as the percentage of a
 * total quantity that have that value. For example,
 * find segments that appear for at least 1% of the cookies.
 *
 * @param support    support percentage
 * @param allOther   include feature for all other values
 */
case class Index(support: Double, allOther: Boolean) extends Transform

/**
 * Break the values in the column into bins with roughly the same number of points.
 *
 * @param nbins target number of bins
 * @param minPoints minimum number of points in single bin
 * @param minPercents minimum percent of points in a bin (0-100).
 *                    The larger of absolute number and percent points is used.
 */
case class Bins(nbins: Int, minPoints: Long = 0, minPercents: Double = 0.0) extends Transform

object Transform {
  def nameOf[T <: Transform : TransformName]: String = implicitly[TransformName[T]].name

  sealed trait TransformName[T <: Transform] {
    def name: String
  }

  object TransformName {
    implicit val identityName = new TransformName[Identity.type] { def name = "identity" }
    implicit val topName = new TransformName[Top] { def name = "top" }
    implicit val indexName = new TransformName[Index] { def name = "index" }
    implicit val binsName = new TransformName[Bins] { def name = "bins"}
  }

  implicit class TransformOps(val transform: Transform) extends AnyVal {
    def stringify: String = transform match {
      case Identity => "identity"
      case _: Top => "top"
      case _: Index => "index"
      case _: Bins => "bins"
    }
  }

  // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
  // *  Parse Transform function from config                                   *
  // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

  import scalaz.syntax.validation._
  import scalaz.syntax.apply._

  def identity(config: Config): ValidationNel[String, Identity.type] = {
    Identity.successNel[String]
  }

  def top(config: Config): ValidationNel[String, Top] = {
    TopParser.parse(config)
  }

  def index(config: Config): ValidationNel[String, Index] = {
    IndexParser.parse(config)
  }

  def bins(config: Config): ValidationNel[String, Bins] = {
    BinsParser.parse(config)
  }

  private abstract class Parser[T <: Transform](transform: String) {

    def parse(config: Config): ValidationNel[String, T]

    protected def integer(p: String)(implicit cfg: Config) =
      parameter(cfg, p)(_.getInt)

    protected def long(p: String)(implicit cfg: Config) =
      parameter(cfg, p)(_.getLong)

    protected def double(p: String)(implicit cfg: Config) =
      parameter(cfg, p)(_.getDouble)

    protected def boolean(p: String)(implicit cfg: Config) =
      parameter(cfg, p)(_.getBoolean)

    private def parameter[P](config: Config, p: String)(f: Config => String => P): ValidationNel[String, P] = {
      Try(f(config)(p)) match {
        case Success(s) => s.successNel
        case Failure(err) =>
          s"Transform: $transform. Failed to load parameter: $p. Error: ${err.getMessage}".failureNel
      }
    }
  }

  private object TopParser extends Parser[Top]("top") {
    def parse(config: Config): ValidationNel[String, Top] = {
      implicit val cfg = config
      (double("cover") |@| boolean("allOther"))(Top.apply)
    }
  }

  private object IndexParser extends Parser[Index]("index") {
    def parse(config: Config): ValidationNel[String, Index] = {
      implicit val cfg = config
      (double("support") |@| boolean("allOther"))(Index.apply)
    }
  }

  private object BinsParser extends Parser[Bins]("bins") {
    def parse(config: Config): ValidationNel[String, Bins] = {
      implicit val cfg = config
      (integer("nbins") |@| long("minpts") |@| double("minpct"))(Bins.apply)
    }
  }

}
