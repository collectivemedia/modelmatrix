package com.collective.modelmatrix.transform

import java.nio.ByteBuffer

import com.collective.modelmatrix.CategorialColumn.CategorialValue
import com.collective.modelmatrix.{CategorialColumn, ModelFeature}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scodec.bits.ByteVector

import scalaz.\/

case class TypedModelFeature(feature: ModelFeature, extractType: DataType)

sealed trait TransformSchemaError {
  def errorMessage: String
}

object TransformSchemaError {

  case class ExtractColumnNotFound(extract: String) extends TransformSchemaError {
    def errorMessage: String = s"Can't find extract column: $extract"
  }

  case class UnsupportedTransformDataType(
    extract: String,
    dataType: DataType,
    transform: Transform
  ) extends TransformSchemaError {
    def errorMessage: String = s"Unsupported input data type: ${dataType.typeName} for transformation: $transform"
  }

}

abstract class Transformer(input: DataFrame) {

  def validate: PartialFunction[ModelFeature, TransformSchemaError \/ TypedModelFeature]

  protected def inputDataType(expression: String): Option[DataType] = {
    input.schema.find(_.name == expression).map(_.dataType)
  }

}

abstract class CategorialTransformer(input: DataFrame) extends Transformer(input) {

  def transform(feature: TypedModelFeature): Seq[CategorialColumn]

  protected case class Value(value: Any, count: Long)

  protected case class Scan(columnId: Int = 0, cumulativeCnt: Long = 0, columns: Seq[CategorialColumn] = Seq.empty)

  protected def valueColumn(extractType: DataType)(
    previousColumnId: Int,
    previousCumCnt: Long,
    value: Value
  ): CategorialValue = value match {
    case Value(s: Short, cnt) if extractType == ShortType =>
      val bb = ByteBuffer.allocate(2).putShort(s)
      CategorialValue(previousColumnId + 1, s.toString, ByteVector(bb), cnt, previousCumCnt + cnt)

    case Value(i: Int, cnt) if extractType == IntegerType =>
      val bb = ByteBuffer.allocate(4).putInt(i)
      CategorialValue(previousColumnId + 1, i.toString, ByteVector(bb), cnt, previousCumCnt + cnt)

    case Value(l: Long, cnt) if extractType == LongType =>
      val bb = ByteBuffer.allocate(8).putLong(l)
      CategorialValue(previousColumnId + 1, l.toString, ByteVector(bb), cnt, previousCumCnt + cnt)

    case Value(d: Double, cnt) if extractType == DoubleType =>
      val bb = ByteBuffer.allocate(8).putDouble(d)
      CategorialValue(previousColumnId + 1, d.toString, ByteVector(bb), cnt, previousCumCnt + cnt)

    case Value(s: String, cnt) if extractType == StringType =>
      CategorialValue(previousColumnId + 1, s.toString, ByteVector(s.getBytes), cnt, previousCumCnt + cnt)

    case Value(v, cnt) =>
      sys.error(s"Unsupported value: $v. Class of: ${v.getClass}. Extract type: $extractType")
  }


}
