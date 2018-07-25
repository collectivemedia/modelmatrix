package com.collective.modelmatrix.cli

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

sealed trait Source {
  def asDataFrame(implicit session: SparkSession): DataFrame
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
  def asDataFrame(implicit session: SparkSession): DataFrame = {
    sys.error(s"Source is not defined")
  }

  override def toString: String = "Source is not defined"
}

case class HiveSource(
  tableName: String
) extends Source {

  def asDataFrame(implicit session: SparkSession): DataFrame = {
    session.sql(s"SELECT * FROM $tableName")
  }

  override def toString: String = {
    s"Hive table: $tableName"
  }

}

case class ParquetSource(
  path: String
) extends Source {

  def asDataFrame(implicit session: SparkSession): DataFrame = {
    session.read.parquet(path)  // #todo
  }

  override def toString: String = {
    s"Parquet: $path"
  }

}
