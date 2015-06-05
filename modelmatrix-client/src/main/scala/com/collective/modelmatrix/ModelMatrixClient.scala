package com.collective.modelmatrix

import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

class ModelMatrixClient(sc: SparkContext) extends ClientModelCatalog {
  private val log = LoggerFactory.getLogger(classOf[ModelMatrixClient])

}
