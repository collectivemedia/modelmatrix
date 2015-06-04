package com.collective.modelmatrix

import java.time.temporal.ChronoField
import java.time.{DayOfWeek, Instant, ZoneId}

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{UDFRegistration, SQLContext}

object ModelMatrix {

  private def registerUDF(udf: UDFRegistration): Unit = {
    udf.register("concat", (sep: String, s1: String, s2: String) => {
      if (sep != null && s1 != null && s2 != null) Some(s"$s1$sep$s2") else None
    })
    udf.register("day_of_week", (ts: Long, zoneId: String) => {
      if (ts > 0) Some(DayOfWeek.from(Instant.ofEpochMilli(ts).atZone(ZoneId.of(zoneId))).toString) else None
    })
    udf.register("hour_of_day", (ts: Long, zoneId: String) => {
      if (ts > 0) Some(Instant.ofEpochMilli(ts).atZone(ZoneId.of(zoneId)).get(ChronoField.HOUR_OF_DAY)) else None
    })
  }

  def sqlContext(sc: SparkContext): SQLContext = {
    val sqlContext = new SQLContext(sc)
    registerUDF(sqlContext.udf)
    sqlContext
  }

  def hiveContext(sc: SparkContext): HiveContext = {
    val sqlContext = new HiveContext(sc)
    registerUDF(sqlContext.udf)
    sqlContext
  }

}
