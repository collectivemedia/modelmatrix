package com.collective.modelmatrix.cli.featurize

import com.collective.modelmatrix.ModelMatrix.PostgresModelMatrixCatalog
import com.collective.modelmatrix.cli.{SourceTransformation, CliSparkContext, Script, Source}
import com.collective.modelmatrix.transform.Transformer
import com.collective.modelmatrix.{Featurization, ModelMatrix}
import org.slf4j.LoggerFactory

import scalaz._

case class ValidateInputData(
  modelInstanceId: Int,
  source: Source,
  repartitionSource: Option[Int],
  cacheSource: Boolean
) extends Script with SourceTransformation with PostgresModelMatrixCatalog with CliSparkContext {

  private val log = LoggerFactory.getLogger(classOf[ValidateInputData])

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {
    log.info(s"Validate input data against Model Matrix instance: $modelInstanceId. " +
      s"Data source: $source")

    implicit val sqlContext = ModelMatrix.hiveContext(sc)

    val features = blockOn(db.run(modelInstanceFeatures.features(modelInstanceId)))
    require(features.nonEmpty, s"No features are defined for model instance: $modelInstanceId. " +
      s"Ensure that this model instance exists")

    val featurization = new Featurization(features)

    val df = toDataFrame(source)

    Transformer.extractFeatures(df, features.map(_.feature)) match {
      // One of extract expressions failed
      case -\/(extractionErrors) =>
        Console.out.println(s"Feature extraction failed:")
        extractionErrors.printASCIITable()

      // Features data frame was successfully prepared
      case \/-(extracted) if featurization.validate(extracted).exists(_.isLeft) =>
        Console.out.println(s"Source can't be featurized because of errors:")
        val validate = featurization.validate(extracted)
        val errors = validate.collect { case -\/(error) => error }
        errors.printASCIITable()

      // All looks good
      case \/-(extracted) =>
        Console.out.println(s"Source schema is compatible with Matrix Model instance: $modelInstanceId")
    }
  }
}
