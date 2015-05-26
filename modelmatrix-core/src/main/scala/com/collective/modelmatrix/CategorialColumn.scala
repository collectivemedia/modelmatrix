package com.collective.modelmatrix

import scodec.bits.ByteVector

sealed trait CategorialColumn {
  type Self <: CategorialColumn

  def columnId: Int
  def count: Long
  def cumulativeCount: Long

  def rebaseColumnId(base: Int): Self
}

object CategorialColumn {

  case class CategorialValue(
    columnId: Int,
    sourceName: String,
    sourceValue: ByteVector,
    count: Long,
    cumulativeCount: Long
  ) extends CategorialColumn {
    type Self = CategorialValue
    def rebaseColumnId(base: Int): CategorialValue = copy(columnId = columnId + base)
  }

  case class AllOther(
    columnId: Int,
    count: Long,
    cumulativeCount: Long
  ) extends CategorialColumn {
    type Self = AllOther
    def rebaseColumnId(base: Int): AllOther = copy(columnId = columnId + base)
  }

}
