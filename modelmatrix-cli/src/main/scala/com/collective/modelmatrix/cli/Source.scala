package com.collective.modelmatrix.cli

import java.nio.file.Path

import org.apache.spark.sql.{SQLContext, DataFrame}

sealed trait Source {
  def asDataFrame(implicit sqlContext: SQLContext): DataFrame
}

object NoSource extends Source {
  def asDataFrame(implicit sqlContext: SQLContext): DataFrame = {
    sys.error(s"Source is not defined")
  }

  override def toString: String = "Source is not defined"
}

case class CsvSource(
  file: Path,
  useHeader: Boolean = true,
  delimiter: Char = ',',
  quote: Char = '"',
  escape: Char = '\\'
) extends Source {

  import com.databricks.spark.csv._

  def asDataFrame(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.csvFile(file.toString, useHeader, delimiter, quote, escape)
  }

  override def toString: String =
    s"CSV file: $file. Use header: $useHeader. Delimiter: $delimiter. Quote: $quote. Escape: $escape"
}
