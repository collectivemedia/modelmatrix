package com.collective.modelmatrix.cli

import org.apache.spark.sql.{SaveMode, DataFrame}

import scala.util.{Failure, Success, Try}

sealed trait Sink {
  def saveDataFrame(df: DataFrame): Unit
}

object Sink {
  private val hive = "hive://(.*)".r
  private val parquet = "parquet://(.*)".r

  def validate(sink: String): Either[String, Unit] = {
    Try(apply(sink)) match {
      case Success(s) => Right(())
      case Failure(err) => Left(s"Unsupported sink type: $sink")
    }
  }

  def apply(sink: String): Sink = sink match {
    case hive(table) => HiveSink(table)
    case parquet(path) => ParquetSink(path)
  }
}

object NoSink extends Sink {
  def saveDataFrame(df: DataFrame): Unit = {
    sys.error(s"Sink is not defined")
  }

  override def toString: String = "Sink is not defined"
}

case class HiveSink(
  tableName: String
) extends Sink {

  def saveDataFrame(df: DataFrame): Unit = {
    df.write.format("hive").mode(SaveMode.Overwrite).saveAsTable(tableName)
  }

  override def toString: String =
    s"Hive table: $tableName"
}

case class ParquetSink(
  path: String
) extends Sink {

  def saveDataFrame(df: DataFrame): Unit = {
    df.write.format("parquet").mode(SaveMode.Overwrite).save(path)
  }

  override def toString: String =
    s"Parquet: $path"
}
