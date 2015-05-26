package com.collective.modelmatrix.cli

import com.collective.modelmatrix.catalog.{ModelDefinitionFeatures, ModelDefinitions, ModelMatrixCatalog}
import com.typesafe.config.Config
import scopt.OptionParser
import slick.driver.PostgresDriver
import slick.driver.PostgresDriver.api._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scalaz._


trait Script {

  def run(): Unit

  protected def blockOn[T](f: Future[T], duration: FiniteDuration = 10.seconds) = {
    Await.result(f, duration)
  }

}

object Script {
  def noOp[T](parser: OptionParser[T]): Script = new Script {
    def run(): Unit = {
      parser.showUsageAsError
    }
  }
}
