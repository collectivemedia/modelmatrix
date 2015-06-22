package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.ModelMatrix
import com.collective.modelmatrix.ModelMatrix.PostgresModelMatrixCatalog
import com.collective.modelmatrix.cli.{Source, _}
import com.collective.modelmatrix.transform._
import org.slf4j.LoggerFactory

import scalaz._

case class ValidateInputData(
  modelDefinitionId: Int,
  source: Source,
  cacheSource: Boolean
) extends Script with PostgresModelMatrixCatalog with CliSparkContext with Transformers {

  private val log = LoggerFactory.getLogger(classOf[ValidateInputData])

  private implicit lazy val sqlContext = ModelMatrix.hiveContext(sc)

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {

    log.info(s"Validate input data against Model Matrix definition: $modelDefinitionId. " +
      s"Data source: $source")

    val features = blockOn(db.run(modelDefinitionFeatures.features(modelDefinitionId))).filter(_.feature.active == true)
    require(features.nonEmpty, s"No active features are defined for model definition: $modelDefinitionId. " +
      s"Ensure that this model definition exists")

    val df = if (cacheSource) source.asDataFrame.cache() else source.asDataFrame
    Transformer.extractFeatures(df, features.map(_.feature)) match {
      // One of extract expressions failed
      case -\/(extractionErrors) =>
        Console.out.println(s"Feature extraction failed:")
        extractionErrors.printASCIITable()

      // Features extracted, time to transform them!
      case \/-(extracted) =>
        val transformers = new Transformers(extracted)

        val validate = features.map(mdf => mdf -> transformers.validate(mdf.feature))

        val featureErrors = validate.collect { case (mdf, -\/(error)) => mdf -> error }
        val typedFeatures = validate.collect { case (mdf, \/-(typed)) => mdf -> typed }

        if (featureErrors.nonEmpty) {
          Console.out.println(s"Input schema errors:")
          featureErrors.printASCIITable()
        }

        if (typedFeatures.nonEmpty) {
          Console.out.println(s"Input schema typed features:")
          typedFeatures.printASCIITable()
        }
    }
  }
}
