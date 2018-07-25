package com.collective.modelmatrix

import org.apache.spark.sql.SparkSession

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

object TestSparkContext {

  lazy implicit val session: SparkSession = {

    val session = SparkSession.builder
      .master("local[1]")
      .appName("Model Matrix Integration Tests")
      .config("spark.local.ip", "localhost")
      .config("spark.driver.host", "localhost")
      .enableHiveSupport()
      .getOrCreate()

      ModelMatrix.registerUDF(session.udf)
      session
   }
}

trait TestSparkContext {

  lazy implicit val session: SparkSession = TestSparkContext.session

  def waitFor[T](f: Future[T], timeout: Duration = 5.second): T = {
    Await.result(f, timeout)
  }

}
