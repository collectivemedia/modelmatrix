package com.collective.modelmatrix.catalog

import java.util.concurrent.Executors

import com.collective.modelmatrix.db.SchemaInstaller
import org.scalatest.BeforeAndAfterAll
import slick.driver.JdbcProfile

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scalaz.Tag

trait CatalogDatabase {
  
  val driver: JdbcProfile

  import driver.api._

  val db: Database

  lazy val catalog = new ModelMatrixCatalog(driver)

  protected implicit val catalogExecutionContext =
    Tag[ExecutionContext, ModelMatrixCatalog](ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10)))

  protected def await[T](f: Future[T], duration: FiniteDuration = 10.seconds): T = {
    Await.result(f, duration)
  }

}

trait InstallSchemaBefore extends SchemaInstaller {
  self: BeforeAndAfterAll with CatalogDatabase =>
  private[this] var schemaInstalled: Boolean = false

  override protected def beforeAll(): Unit = {
    this.synchronized {
      if (!schemaInstalled) {
        installOrMigrate
        schemaInstalled = true
      }
    }
  }
}



