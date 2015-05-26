package com.collective.modelmatrix.cli

import org.apache.spark.{SparkContext, SparkConf}

trait CliSparkContext {

  lazy val sc = {
    val conf = new SparkConf()
      .setAppName("Model Matrix")
      .setMaster("local[2]")
    new SparkContext(conf)
  }

}
