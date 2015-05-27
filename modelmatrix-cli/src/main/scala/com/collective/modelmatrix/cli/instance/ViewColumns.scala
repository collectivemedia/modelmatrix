package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.CategorialColumn
import com.collective.modelmatrix.catalog._
import com.collective.modelmatrix.cli._
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._


case class ViewColumns(
  modelInstanceId: Int, dbName: String, dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog {

  private val log = LoggerFactory.getLogger(classOf[ViewColumns])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"View Model Matrix instance columns: $modelInstanceId. Database: $dbName @ ${dbConfig.origin().filename()}")

    blockOn(db.run(modelInstances.findById(modelInstanceId))) match {
      case Some(modelInstance) =>

        val features = blockOn(db.run(modelInstanceFeatures.features(modelInstanceId)))
        val columns: Seq[(ModelInstanceFeature, Option[CategorialColumn])] = features flatMap {
          case f: ModelInstanceIdentityFeature => Seq((f, None))
          case f: ModelInstanceTopFeature => f.columns.map(c => (f, Some(c)))
          case f: ModelInstanceIndexFeature => f.columns.map(c => (f, Some(c)))
        }

        Console.out.println(s"Model instance:")
        modelInstance.printASCIITable()

        Console.out.println(s"Model Matrix instance columns: ${columns.length}")
        columns.printASCIITable()

      case None =>
        Console.out.println(s"Can't find model instance by id: $modelInstanceId")
    }
  }
}
