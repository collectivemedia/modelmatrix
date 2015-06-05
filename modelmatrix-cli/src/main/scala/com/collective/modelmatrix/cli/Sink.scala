package com.collective.modelmatrix.cli

import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

sealed trait Sink {
  def saveDataFrame(df: DataFrame)(implicit sqlContext: SQLContext): Unit
}

object Sink {
  private val csv = "csv://(.*)".r
  private val hive = "hive://(.*)".r
  private val parquet = "parquet://(.*)".r

  def validate(sink: String): Either[String, Unit] = {
    Try(apply(sink)) match {
      case Success(s) => Right(())
      case Failure(err) => Left(s"Unsupported sink type: $sink")
    }
  }

  def apply(sink: String): Sink = sink match {
    case csv(path) => CsvSink(path)
    case hive(table) => HiveSink(table)
    case parquet(path) => ParquetSink(path)
  }
}

object NoSink extends Sink {
  def saveDataFrame(df: DataFrame)(implicit sqlContext: SQLContext): Unit = {
    sys.error(s"Sink is not defined")
  }

  override def toString: String = "Sink is not defined"
}

case class CsvSink(
  path: String,
  useHeader: Boolean = true,
  delimiter: Char = ',',
  quote: Char = '"',
  escape: Char = '\\'
) extends Sink {

  import com.databricks.spark.csv._

  private val parameters = Map(
    "header" -> useHeader.toString,
    "delimiter" -> delimiter.toString,
    "quote" -> quote.toString,
    "escape" -> escape.toString
  )
  def saveDataFrame(df: DataFrame)(implicit sqlContext: SQLContext): Unit = {
    df.saveAsCsvFile(path, parameters)
  }

  override def toString: String =
    s"CSV: $path"
}

case class HiveSink(
  tableName: String
) extends Sink {

  def saveDataFrame(df: DataFrame)(implicit sqlContext: SQLContext): Unit = {
    df.saveAsTable(tableName, SaveMode.Overwrite)
  }

  override def toString: String =
    s"Hive table: $tableName"
}

case class ParquetSink(
  path: String
) extends Sink {

  def saveDataFrame(df: DataFrame)(implicit sqlContext: SQLContext): Unit = {
    df.saveAsParquetFile(path)
  }

  override def toString: String =
    s"Parquet: $path"
}
