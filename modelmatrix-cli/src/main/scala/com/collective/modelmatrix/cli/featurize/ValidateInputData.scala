package com.collective.modelmatrix.cli.featurize

import com.collective.modelmatrix.FeatureExtraction
import com.collective.modelmatrix.catalog.ModelMatrixCatalog
import com.collective.modelmatrix.cli.{CliModelCatalog, CliSparkContext, Script, Source}
import com.collective.modelmatrix.transform.Transformer
import com.typesafe.config.Config
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._

case class ValidateInputData(
  modelInstanceId: Int,
  source: Source,
  dbName: String,
  dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog with CliSparkContext {

  private val log = LoggerFactory.getLogger(classOf[ValidateInputData])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"Validate input data against Model Matrix instance: $modelInstanceId. " +
      s"Data source: $source. " +
      s"Database: $dbName @ ${dbConfig.origin()}")

    implicit val sqlContext = new HiveContext(sc)

    val features = blockOn(db.run(modelInstanceFeatures.features(modelInstanceId)))
    require(features.nonEmpty, s"No active features are defined for model instance: $modelInstanceId. " +
      s"Ensure that this model instance exists")

    val featuresExtraction = new FeatureExtraction(features)

    val input = Transformer.selectFeatures(source.asDataFrame, features.map(_.feature))
    val validate = featuresExtraction.validate(input)

    // Print schema errors
    val invalidFeatures = validate.collect { case -\/(error) => error }
    if (invalidFeatures.nonEmpty) {
      Console.out.println(s"Input schema errors:")
      invalidFeatures.printASCIITable()

    } else {
      Console.out.println(s"Input schema is compatible with Matrix Model instance: $modelInstanceId")
    }
  }
}
