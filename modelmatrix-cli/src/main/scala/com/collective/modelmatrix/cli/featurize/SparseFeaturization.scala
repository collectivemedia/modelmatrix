package com.collective.modelmatrix.cli.featurize

import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{Source, _}
import com.collective.modelmatrix.transform.Transformer
import com.collective.modelmatrix.{FeatureExtraction, IdentifiedPoint}
import com.typesafe.config.Config
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._

case class SparseFeaturization(
  modelInstanceId: Int,
  source: Source,
  sink: Sink,
  idColumn: String,
  dbName: String,
  dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog with CliSparkContext {

  private val log = LoggerFactory.getLogger(classOf[ValidateInputData])

  private def sparseSchema(idType: DataType) = StructType(Seq(
    StructField(idColumn, idType),
    StructField("column_id", IntegerType),
    StructField("value", DoubleType)
  ))

  def run(): Unit = {

    log.info(s"Run sparse featurization using Model Matrix instance: $modelInstanceId. " +
      s"Input source: $source. " +
      s"Featurized sink: $sink. " +
      s"Id column: $idColumn" +
      s"Database: $dbName @ ${dbConfig.origin()}")

    implicit val sqlContext = new HiveContext(sc)

    val features = blockOn(db.run(modelInstanceFeatures.features(modelInstanceId)))
    require(features.nonEmpty, s"No active features are defined for model instance: $modelInstanceId. " +
      s"Ensure that this model instance exists")

    // Do featurization
    val featuresExtraction = new FeatureExtraction(features)
    val input = Transformer.selectFeaturesWithId(source.asDataFrame, idColumn, features.map(_.feature))
    val (idType, featurized) = featuresExtraction.featurize(input, idColumn)

    // Switch from 0-based Vector index to 1-based ColumnId
    val rows = featurized.flatMap {
      case IdentifiedPoint(id, sparse: SparseVector) =>
        (sparse.values zip sparse.indices).map { case (value, idx) => Row(id, idx + 1, value) }
      case IdentifiedPoint(id, dense: DenseVector) =>
        dense.values.zipWithIndex.map { case (value, idx) => Row(id, idx + 1, value)}
    }

    // Apply schema and save
    sink.saveDataFrame(sqlContext.createDataFrame(rows, sparseSchema(idType)))

    Console.out.println(s"Featurized data set was successfully saved to: $sink")
  }
}
