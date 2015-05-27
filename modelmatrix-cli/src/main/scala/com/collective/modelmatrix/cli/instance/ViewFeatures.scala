package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{CliModelCatalog, Script}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._


case class ViewFeatures(
  modelInstanceId: Int, dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[ViewFeatures])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"View Model Matrix instance features: $modelInstanceId. Database: $dbName @ ${dbConfig.origin().filename()}")

    blockOn(db.run(modelInstances.findById(modelInstanceId))) match {
      case Some(modelInstance) =>
        val features = blockOn(db.run(modelInstanceFeatures.features(modelInstanceId)))

        Console.out.println(s"Model instance:")
        modelInstance.printASCIITable()

        Console.out.println(s"Model Matrix instance features: ${features.length}")
        features.printASCIITable()

      case None =>
        Console.out.println(s"Can't find model instance by id: $modelInstanceId")
    }
  }
}
