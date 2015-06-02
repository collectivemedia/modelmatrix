package com.collective.modelmatrix

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.types._
import scodec.bits.ByteVector
import slick.driver.PostgresDriver.api._

package object catalog {

  implicit val instantColumnType =
    MappedColumnType.base[Instant, java.sql.Timestamp](
      instant => Timestamp.from(instant),
      _.toInstant
    )

  implicit val dataTypeColumnType =
    MappedColumnType.base[DataType, String]({
      case ShortType => "short"
      case IntegerType => "integer"
      case LongType => "long"
      case DoubleType => "double"
      case StringType => "string"
    }, {
      case "short" => ShortType
      case "integer" => IntegerType
      case "long" => LongType
      case "double" => DoubleType
      case "string" => StringType
    })

  implicit val byteVectorColumnType =
    MappedColumnType.base[ByteVector, Array[Byte]](
      _.toArray,
      ByteVector.apply
    )
}
