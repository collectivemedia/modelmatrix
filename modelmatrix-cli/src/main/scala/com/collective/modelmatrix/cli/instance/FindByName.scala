package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{CliModelCatalog, Script}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._

case class FindByName(
  name: String, dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[FindByName])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"Find Model Matrix instance by name: $name. Database: $dbName @ ${dbConfig.origin().filename()}")
    blockOn(db.run(modelInstances.findByName(name))).printASCIITable()
  }
}
