package com.collective.modelmatrix.cli.definition

import com.collective.modelmatrix.ModelMatrix.DbModelMatrixCatalog
import com.collective.modelmatrix.cli.Script
import org.slf4j.LoggerFactory

case class ListDefinitions(
  name: Option[String]
) extends Script with DbModelMatrixCatalog {

  private val log = LoggerFactory.getLogger(classOf[ListDefinitions])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"List Model Matrix definitions. " +
      s"Name: ${name.getOrElse("-")}")
    blockOn(db.run(modelDefinitions.list(name))).printASCIITable()
  }
}
