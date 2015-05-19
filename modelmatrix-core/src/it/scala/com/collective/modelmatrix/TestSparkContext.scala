package com.collective.modelmatrix

import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

object TestSparkContext {

  private[this] val conf =
    new SparkConf()
      .setMaster("local[2]")
      .set("spark.local.ip","localhost")
      .set("spark.driver.host","localhost")
      .setAppName("Model Matrix Integration Tests")

  lazy val sc: SparkContext = new SparkContext(conf)
}


trait TestSparkContext {

  lazy val sc: SparkContext = TestSparkContext.sc

  def waitFor[T](f: Future[T], timeout: Duration = 5.second): T = {
    Await.result(f, timeout)
  }

}
