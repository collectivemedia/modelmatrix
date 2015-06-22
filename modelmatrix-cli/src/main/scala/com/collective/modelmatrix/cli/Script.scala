package com.collective.modelmatrix.cli

import scopt.OptionParser

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future}


trait Script {

  def run(): Unit

  def as[T]: T = this.asInstanceOf[T]

}

object Script {
  def noOp[T](parser: OptionParser[T]): Script = new Script {
    def run(): Unit = {
      parser.showUsageAsError
    }
  }
}
