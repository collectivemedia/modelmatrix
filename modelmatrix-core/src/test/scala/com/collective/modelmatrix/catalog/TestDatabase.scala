package com.collective.modelmatrix.catalog

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import slick.driver.{H2Driver, JdbcProfile}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

trait TestDatabase {
  
  val driver: JdbcProfile
  
  import driver.api._
  
  val db: Database

  protected def await[T](f: Future[T], duration: FiniteDuration = 10.seconds): T = {
    Await.result(f, duration)
  }

}

trait H2Database extends TestDatabase {
  self: FlatSpec with BeforeAndAfterAll =>

  val driver = H2Driver
  
  import driver.api._

  val db = Database.forConfig("h2dev", ConfigFactory.load("./h2.conf"))

  def createSchemas: Seq[DBIO[Unit]]

  override protected def beforeAll(): Unit = {
    await(db.run(DBIO.seq(createSchemas:_*)))
  }

  override protected def afterAll(): Unit = {
    db.close()
  }

}
