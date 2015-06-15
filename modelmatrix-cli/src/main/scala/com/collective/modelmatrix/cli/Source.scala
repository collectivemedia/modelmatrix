package com.collective.modelmatrix.cli

import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

sealed trait Source {
  def asDataFrame(implicit sqlContext: SQLContext): DataFrame
}

object Source {
  private val hive = "hive://(.*)".r
  private val parquet = "parquet://(.*)".r

  def validate(source: String): Either[String, Unit] = {
    Try(apply(source)) match {
      case Success(s) => Right(())
      case Failure(err) => Left(s"Unsupported source type: $source")
    }
  }

  def apply(source: String): Source = source match {
    case hive(table) => HiveSource(table)
    case parquet(path) => ParquetSource(path)
  }
}

object NoSource extends Source {
  def asDataFrame(implicit sqlContext: SQLContext): DataFrame = {
    sys.error(s"Source is not defined")
  }

  override def toString: String = "Source is not defined"
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

case class ParquetSource(
  path: String
) extends Source {

  def asDataFrame(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.parquetFile(path)
  }

  override def toString: String = {
    s"Parquet: $path"
  }

}
