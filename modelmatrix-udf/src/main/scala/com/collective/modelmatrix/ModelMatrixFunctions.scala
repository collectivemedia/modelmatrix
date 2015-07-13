package com.collective.modelmatrix

import java.time.temporal.ChronoField
import java.time.{DayOfWeek, Instant, ZoneId}


class ModelMatrixFunctions {

  def nvl[T](v: T, default: T): T = {
    if (v == null) default else v
  }

  def dayOfWeek(epochMilli: Long, zoneId: ZoneId): Int = {
    DayOfWeek.from(Instant.ofEpochMilli(epochMilli).atZone(zoneId)).getValue
  }

  def hourOfDay(epochMilli: Long, zoneId: ZoneId): Integer = {
    Instant.ofEpochMilli(epochMilli).atZone(zoneId).get(ChronoField.HOUR_OF_DAY)
  }

  def concat(separator: String, s1: String, s2: String): String = {
    s"$s1$separator$s2"
  }

}

object ModelMatrixFunctions extends ModelMatrixFunctions
