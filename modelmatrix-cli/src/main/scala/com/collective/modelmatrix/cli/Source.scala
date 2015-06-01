package com.collective.modelmatrix.cli

import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

sealed trait Source {
  def asDataFrame(implicit sqlContext: SQLContext): DataFrame
}

object Source {
  private val csv = "csv://(.*)".r
  private val hive = "hive://(.*)".r

  def validate(source: String): Either[String, Unit] = {
    Try(apply(source)) match {
      case Success(s) => Right(())
      case Failure(err) => Left(s"Unsupported source type: $source")
    }
  }

  def apply(source: String): Source = source match {
    case csv(path) => CsvSource(path)
    case hive(path) => HiveSource(path)
  }
}

object NoSource extends Source {
  def asDataFrame(implicit sqlContext: SQLContext): DataFrame = {
    sys.error(s"Source is not defined")
  }

  override def toString: String = "Source is not defined"
}

case class CsvSource(
  path: String,
  useHeader: Boolean = true,
  delimiter: Char = ',',
  quote: Char = '"',
  escape: Char = '\\'
) extends Source {

  import com.databricks.spark.csv._

  def asDataFrame(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.csvFile(path, useHeader, delimiter, quote, escape)
  }

  override def toString: String = {
    s"CSV file: $path"
  }

}

case class HiveSource(
  tableName: String
) extends Source {

  def asDataFrame(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.sql(s"SELECT * FROM $tableName")
  }

  override def toString: String = {
    s"Hive table: $tableName"
  }

}
