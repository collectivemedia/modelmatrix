package com.collective.modelmatrix.cli

import scopt.OptionParser

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future}


trait Script {

  def run(): Unit

  def as[T]: T = this.asInstanceOf[T]

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
