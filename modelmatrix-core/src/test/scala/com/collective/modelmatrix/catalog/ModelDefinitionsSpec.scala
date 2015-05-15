package com.collective.modelmatrix.catalog

import java.time.Instant

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import slick.driver.H2Driver

import scala.concurrent.ExecutionContext.Implicits.global

class H2ModelDefinitionsSpec extends ModelDefinitionsSpec with H2Database {
  val createSchemas = Seq(modelDefinitions.createSchema)
}

trait ModelDefinitionsSpec extends FlatSpec with BeforeAndAfterAll with TestDatabase {

  val modelDefinitions = new ModelDefinitions(H2Driver)

  val now = Instant.now()

  "Model Definitions" should "add model definition and read it later" in {

    val id = await(db.run(modelDefinitions.add(
      source = "source",
      createdBy = "ModelDefinitionsSpec",
      createdAt = now,
      comment = Some("testing")
    )))

    val all = await(db.run(modelDefinitions.all))
    assert(all.nonEmpty)

    val byIdOpt = all.find(_.id == id)
    assert(byIdOpt.isDefined)

    val byId = byIdOpt.get
    assert(byId.createdBy == "ModelDefinitionsSpec")
    assert(byId.createdAt == now)
  }
}


