package com.collective.modelmatrix

case class BinColumn(
  columnId: Int,
  low: Double,
  high: Double,
  count: Long,
  sampleSize: Long
) {

  def rebaseColumnId(base: Int): BinColumn = copy(columnId = columnId + base)

  def fallIntoThisBin(value: Double): Boolean = {
    low <= value && value < high
  }

}
