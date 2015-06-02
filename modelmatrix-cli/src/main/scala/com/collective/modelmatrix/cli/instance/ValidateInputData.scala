package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.ModelFeature
import com.collective.modelmatrix.catalog.{ModelDefinitionFeature, ModelMatrixCatalog}
import com.collective.modelmatrix.cli.{Source, _}
import com.collective.modelmatrix.transform._
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scalaz._

case class ValidateInputData(
  modelDefinitionId: Int,
  source: Source,
  dbName: String,
  dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog with CliSparkContext {

  private val log = LoggerFactory.getLogger(classOf[ValidateInputData])

  private implicit lazy val sqlContext = new HiveContext(sc)

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  private class Transformers(input: DataFrame) {
    private implicit val sqlContext = new HiveContext(sc)

    private val identity = new IdentityTransformer(input)
    private val top = new TopTransformer(input)
    private val index = new IndexTransformer(input)
    private val bins = new BinsTransformer(input)

    private val unknownFeature: PartialFunction[ModelFeature, TransformSchemaError \/ TypedModelFeature] = {
      case feature => sys.error(s"Feature can't be validated by any of transformers: $feature")
    }

    def validate(feature: ModelFeature): TransformSchemaError \/ TypedModelFeature =
      (identity.validate orElse
       top.validate orElse
       index.validate orElse
       bins.validate orElse
       unknownFeature
      )(feature)
  }

  def run(): Unit = {

    log.info(s"Validate input data against Model Matrix definition: $modelDefinitionId. " +
      s"Data source: $source. " +
      s"Database: $dbName @ ${dbConfig.origin()}")

    val features = blockOn(db.run(modelDefinitionFeatures.features(modelDefinitionId))).filter(_.feature.active == true)
    require(features.nonEmpty, s"No active features are defined for model definition: $modelDefinitionId. " +
      s"Ensure that this model definition exists")

    // Cache feature columns
    val input = Transformer.selectFeatures(source.asDataFrame, features.map(_.feature)).cache()
    val transformers = new Transformers(input)

    // Validate each feature
    val validate = features.filter(_.feature.active).map { case mdf@ModelDefinitionFeature(_, _, feature) =>
      mdf -> transformers.validate(feature)
    }

    // Print schema errors
    val invalidFeatures = validate.collect { case (mdf, -\/(error)) => mdf -> error }
    if (invalidFeatures.nonEmpty) {
      Console.out.println(s"Input schema errors:")
      invalidFeatures.printASCIITable()
    }

    // Print schema typed features
    val typedFeatures = validate.collect { case (mdf, \/-(typed)) => mdf -> typed }
    if (typedFeatures.nonEmpty) {
      Console.out.println(s"Input schema typed features:")
      typedFeatures.printASCIITable()
    }
  }
}
