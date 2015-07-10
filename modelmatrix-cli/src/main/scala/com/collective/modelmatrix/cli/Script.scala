package com.collective.modelmatrix.cli

import org.apache.spark.sql.{SQLContext, DataFrame}
import scopt.OptionParser


trait Script {

  def run(): Unit

  def as[T]: T = this.asInstanceOf[T]

}

object Script {
  def noOp[T](parser: OptionParser[T]): Script = new Script {
    def run(): Unit = {
      parser.showUsageAsError
    }
  }
}

trait SourceTransformation { self: Script =>

  def repartitionSource: Option[Int]
  def cacheSource: Boolean

  private type TransformSource = DataFrame => DataFrame

  private val cache: TransformSource = df => if (cacheSource) df.cache() else df
  private val repartition: TransformSource = df => repartitionSource.map(df.repartition).getOrElse(df)

  protected def toDataFrame(source: Source)(implicit sqlContext: SQLContext): DataFrame = {
    (repartition andThen cache)(source.asDataFrame)
  }
}
