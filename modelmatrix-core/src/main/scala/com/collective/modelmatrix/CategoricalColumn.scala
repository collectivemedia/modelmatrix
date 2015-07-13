package com.collective.modelmatrix

import scodec.bits.ByteVector

sealed trait CategoricalColumn {
  type Self <: CategoricalColumn

  def columnId: Int
  def count: Long
  def cumulativeCount: Long

  def rebaseColumnId(base: Int): Self
}

object CategoricalColumn {

  case class CategoricalValue(
    columnId: Int,
    sourceName: String,
    sourceValue: ByteVector,
    count: Long,
    cumulativeCount: Long
  ) extends CategoricalColumn {
    type Self = CategoricalValue
    def rebaseColumnId(base: Int): CategoricalValue = copy(columnId = columnId + base)
  }

  case class AllOther(
    columnId: Int,
    count: Long,
    cumulativeCount: Long
  ) extends CategoricalColumn {
    type Self = AllOther
    def rebaseColumnId(base: Int): AllOther = copy(columnId = columnId + base)
  }

}
