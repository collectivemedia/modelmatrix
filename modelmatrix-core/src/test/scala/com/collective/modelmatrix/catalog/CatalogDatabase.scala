package com.collective.modelmatrix.catalog

import java.util.concurrent.Executors

import slick.driver.{H2Driver, JdbcProfile}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scalaz.Tag

trait CatalogDatabase {
  
  val driver: JdbcProfile
  
  import driver.api._
  
  val db: Database

  lazy val schema = new Schema(H2Driver)

  protected implicit val catalogExecutionContext =
    Tag[ExecutionContext, ModelMatrixCatalog](ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10)))

  protected def await[T](f: Future[T], duration: FiniteDuration = 10.seconds): T = {
    Await.result(f, duration)
  }

}
