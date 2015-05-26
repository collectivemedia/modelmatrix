package com.collective.modelmatrix

import scodec.bits.ByteVector

sealed trait CategorialColumn {
  def columnId: Int
}

object CategorialColumn {

  case class CategorialValue(
    columnId: Int,
    sourceName: String,
    sourceValue: ByteVector,
    count: Long,
    cumulativeCount: Long
  ) extends CategorialColumn

  case class AllOther(
    columnId: Int,
    count: Long,
    cumulativeCount: Long
  ) extends CategorialColumn

}
