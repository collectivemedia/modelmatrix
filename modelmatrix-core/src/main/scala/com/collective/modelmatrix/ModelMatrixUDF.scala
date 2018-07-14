package com.collective.modelmatrix

import java.time.ZoneId
import java.{lang => jl}

import org.apache.spark.sql.UDFRegistration

trait ModelMatrixUDF {

  private val dayOfWeek: (jl.Long, String) => jl.Integer = {
    case (ts, zoneId) if ts != null => ModelMatrixFunctions.dayOfWeek(ts, ZoneId.of(zoneId))
    case _ => null
  }

  private val hourOfDay: (jl.Long, String) => jl.Integer = {
    case (ts, zoneId) if ts != null => ModelMatrixFunctions.hourOfDay(ts, ZoneId.of(zoneId))
    case _ => null
  }

  private val nvlString: (String, String) => String = ModelMatrixFunctions.nvl[String]

  private val nvl: (jl.Double, jl.Double) => jl.Double = ModelMatrixFunctions.nvl[jl.Double]

  private val strToEpochMilli: String => jl.Long = {
    case s if s != null => s.toLong
    case _ => null
  }

  private val concat: (String, String, String) => String = {
    case (sep, s1, s2) if sep != null && s1 != null && s2 != null => ModelMatrixFunctions.concat(sep, s1, s2)
    case _ => null
  }

  private val log: jl.Double => jl.Double = {
    case d if d != null => math.log(d).asInstanceOf[jl.Double]
    case _ => null
  }

  private val greatest: (jl.Double, jl.Double) => jl.Double = {
    case (l, r) if l != null && r != null =>
      math.max(l, r).asInstanceOf[jl.Double]
    case _ => null
  }

  def registerUDF(udf: UDFRegistration): Unit = {
    udf.register("strToEpochMilli", strToEpochMilli)
    udf.register("concat", concat)
    udf.register("day_of_week", dayOfWeek)
    udf.register("hour_of_day", hourOfDay)
    udf.register("nvl_str", nvlString)
    udf.register("nvl", nvl)
    udf.register("log", log)
    udf.register("greatest", greatest)
  }

}
