package com.collective.modelmatrix.cli

import com.collective.modelmatrix.ModelMatrix
import org.apache.spark.sql.SparkSession

trait CliSparkContext {

  lazy implicit val session: SparkSession = {

    val session = SparkSession.builder.
      master("local")
      .appName("modelmatrx")
      .enableHiveSupport()
      .getOrCreate()

      ModelMatrix.registerUDF(session.udf)
      session
   }
}
