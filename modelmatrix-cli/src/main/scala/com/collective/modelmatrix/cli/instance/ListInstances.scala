package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.ModelMatrix.PostgresModelMatrixCatalog
import com.collective.modelmatrix.cli.Script
import org.slf4j.LoggerFactory


case class ListInstances(
  modelDefinitionId: Option[Int], name: Option[String]
) extends Script with PostgresModelMatrixCatalog {

  private val log = LoggerFactory.getLogger(classOf[ListInstances])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {

    log.info(s"List Model Matrix instances. " +
      s"Definition id: ${modelDefinitionId.getOrElse("-")}. " +
      s"Name: ${name.getOrElse("-")}")

    blockOn(db.run(modelInstances.list(modelDefinitionId, name))).printASCIITable()
  }
}
