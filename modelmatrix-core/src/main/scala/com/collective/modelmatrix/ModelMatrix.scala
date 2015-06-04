package com.collective.modelmatrix

import java.time.temporal.ChronoField
import java.time.{Instant, DayOfWeek, ZoneId}

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, UDFRegistration}

object ModelMatrix {

  private val toLong: String => java.lang.Long = {
    case s if s != null => s.toLong
    case _ => null
  }

  private val concat: (String, String, String) => String = {
    case (sep, s1, s2) if sep != null && s1 != null && s2 != null => s"$s1$sep$s2"
    case _ => null
  }

  private val dayOfWeek: (java.lang.Long, String) => String = {
    case (ts, zoneId) if ts != null =>
      DayOfWeek.from(Instant.ofEpochMilli(ts).atZone(ZoneId.of(zoneId))).toString
    case _ => null
  }

  private val hourOfDay: (java.lang.Long, String) => java.lang.Integer = {
    case (ts, zoneId) if ts != null =>
      Instant.ofEpochMilli(ts).atZone(ZoneId.of(zoneId)).get(ChronoField.HOUR_OF_DAY)
    case _ => null
  }

  private val nvlString: (String, String) => String = {
    case (s, default) if s == null => default
    case (s, _) => s
  }

  private val nvl: (java.lang.Double, java.lang.Double) => java.lang.Double = {
    case (d, default) if d == null => default
    case (d, _) => d
  }

  private val log: java.lang.Double => java.lang.Double = {
    case d if d != null => math.log(d).asInstanceOf[java.lang.Double]
    case _ => null
  }

  private val greatest: (java.lang.Double, java.lang.Double) => java.lang.Double = {
    case (l, r) if l != null && r != null =>
      math.max(l, r).asInstanceOf[java.lang.Double]
    case _ => null
  }

  private def registerUDF(udf: UDFRegistration): Unit = {
    udf.register("toLong", toLong)
    udf.register("concat", concat)
    udf.register("day_of_week", dayOfWeek)
    udf.register("hour_of_day", hourOfDay)
    udf.register("nvl_str", nvlString)
    udf.register("nvl", nvl)
    udf.register("log", log)
    udf.register("greatest", greatest)
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
