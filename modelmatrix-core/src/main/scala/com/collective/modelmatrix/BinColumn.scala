package com.collective.modelmatrix

sealed trait BinColumn {
  type Self <: BinColumn

  def columnId: Int
  def count: Long
  def sampleSize: Long

  def rebaseColumnId(base: Int): Self
  def fallIntoThisBin(value: Double): Boolean
}

object BinColumn {

  def toLowerBin(v: BinValue): LowerBin =
    LowerBin(v.columnId, v.high, v.count, v.sampleSize)

  def toUpperBin(v: BinValue): UpperBin =
    UpperBin(v.columnId, v.low, v.count, v.sampleSize)

  case class LowerBin(
    columnId: Int,
    high: Double,
    count: Long,
    sampleSize: Long
  ) extends BinColumn {
    type Self = LowerBin

    def rebaseColumnId(base: Int): LowerBin =
      copy(columnId = base + columnId)

    def fallIntoThisBin(value: Double): Boolean =
      value < high
  }

  case class UpperBin(
    columnId: Int,
    low: Double,
    count: Long,
    sampleSize: Long
  ) extends BinColumn {
    type Self = UpperBin

    def rebaseColumnId(base: Int): UpperBin =
      copy(columnId = base + columnId)

    def fallIntoThisBin(value: Double): Boolean =
      value >= low
  }

  case class BinValue(
    columnId: Int,
    low: Double,
    high: Double,
    count: Long,
    sampleSize: Long
  ) extends BinColumn {
    type Self = BinValue

    def rebaseColumnId(base: Int): BinValue =
      copy(columnId = base + columnId)

    def fallIntoThisBin(value: Double): Boolean =
      low <= value && value < high
  }

}
