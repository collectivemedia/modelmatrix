package com.collective.modelmatrix.catalog

import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import slick.driver.H2Driver.api._

trait DatabaseSpec extends FlatSpec with BeforeAndAfterAll {

  val db = Database.forConfig("h2dev")

  def createSchemas: Seq[DBIO[Unit]]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    db.run(DBIO.seq(createSchemas:_*))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    db.close()
  }

  protected def await[T](f: Future[T], duration: FiniteDuration = 10.seconds): T = {
    Await.result(f, duration)
  }

}
