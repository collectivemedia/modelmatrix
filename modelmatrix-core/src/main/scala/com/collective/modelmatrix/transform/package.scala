package com.collective.modelmatrix

import org.apache.spark.sql.DataFrame


package object transform {

  implicit class DataFrameOps(val df: DataFrame) extends AnyVal {
    def sumOf(name: String): Long = {
      import org.apache.spark.sql.functions._
      val row = df.select(sum(col(name))).first()
      if (row.isNullAt(0)) 0 else row.getLong(0)
    }
  }

}
