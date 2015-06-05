package com.collective.modelmatrix

import java.util.concurrent.Executors

import com.collective.modelmatrix.catalog._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.config.{Config, ConfigFactory}
import slick.driver.PostgresDriver

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scalaz._

private[modelmatrix] trait ClientModelCatalog {

  protected val driver = slick.driver.PostgresDriver
  import driver.api._

  private val dbName: String = "modelmatrix.catalog.db"
  private val dbConfig: Config = ConfigFactory.load()

  protected implicit val ec: ExecutionContext @@ ModelMatrixCatalog =
    Tag[ExecutionContext, ModelMatrixCatalog](ExecutionContext.fromExecutor(
      Executors.newFixedThreadPool(10, threadFactory("catalog-db-pool", daemon = true)))
    )

  protected lazy val db = Database.forConfig(dbName, dbConfig)
  protected lazy val catalog = new ModelMatrixCatalog(PostgresDriver)

  protected lazy val modelDefinitions = new ModelDefinitions(catalog)
  protected lazy val modelDefinitionFeatures = new ModelDefinitionFeatures(catalog)

  protected lazy val modelInstances = new ModelInstances(catalog)
  protected lazy val modelInstanceFeatures = new ModelInstanceFeatures(catalog)

  protected def blockOn[T](f: Future[T], duration: FiniteDuration = 10.seconds) = {
    Await.result(f, duration)
  }

  private def threadFactory(prefix: String, daemon: Boolean) =
    new ThreadFactoryBuilder().
      setDaemon(daemon).
      setNameFormat(s"$prefix-%d").
      build()
}

