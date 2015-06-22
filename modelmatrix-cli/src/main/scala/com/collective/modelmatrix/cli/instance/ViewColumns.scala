package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.ModelMatrix.PostgresModelMatrixCatalog
import com.collective.modelmatrix.catalog._
import com.collective.modelmatrix.cli._
import com.collective.modelmatrix.{BinColumn, CategorialColumn}
import org.slf4j.LoggerFactory

import scalaz._


case class ViewColumns(
  modelInstanceId: Int, group: Option[String], feature: Option[String]
) extends Script with PostgresModelMatrixCatalog {

  private val log = LoggerFactory.getLogger(classOf[ViewColumns])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"View Model Matrix instance columns: $modelInstanceId. " +
      s"Feature filter: ${feature.getOrElse("")}")

    blockOn(db.run(modelInstances.findById(modelInstanceId))) match {
      case Some(modelInstance) =>

        val groupFilter = if (group.isDefined) (_: String) == group.get else (_: String) => true
        val featureFilter = if (feature.isDefined) (_: String) == feature.get else (_: String) => true

        val features = blockOn(db.run(modelInstanceFeatures.features(modelInstanceId)))
          .filter(f => featureFilter(f.feature.feature) && groupFilter(f.feature.group))

        val columns: Seq[(ModelInstanceFeature, Option[CategorialColumn \/ BinColumn])] = features flatMap {
          case f: ModelInstanceIdentityFeature => Seq((f, None))
          case f: ModelInstanceTopFeature => f.columns.map(c => (f, Some(\/.left(c))))
          case f: ModelInstanceIndexFeature => f.columns.map(c => (f, Some(\/.left(c))))
          case f: ModelInstanceBinsFeature => f.columns.map(c => (f, Some(\/.right(c))))
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
