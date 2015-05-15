package com.collective.modelmatrix.cli

import scopt.OptionParser

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

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
