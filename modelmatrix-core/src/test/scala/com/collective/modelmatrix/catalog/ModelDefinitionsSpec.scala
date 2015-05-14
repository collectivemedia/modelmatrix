package com.collective.modelmatrix.catalog

import slick.driver.H2Driver

import scala.concurrent.ExecutionContext.Implicits.global

class ModelDefinitionsSpec extends DatabaseSpec {

  def createSchemas = Seq(modelDefinitions.createSchema)

  val modelDefinitions = new ModelDefinitions(H2Driver)

  "Model Definitions" should "return empty definitions list after startup" in {
    val all = await(db.run(modelDefinitions.all))
    assert(all.isEmpty)
  }
}
