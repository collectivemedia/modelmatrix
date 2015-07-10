package com.collective.modelmatrix.cli.instance

import com.collective.modelmatrix.ModelMatrix.PostgresModelMatrixCatalog
import com.collective.modelmatrix._
import com.collective.modelmatrix.cli.{SourceTransformation, CliSparkContext, Script, Source}
import com.collective.modelmatrix.transform._
import org.slf4j.LoggerFactory

import scalaz.{-\/, \/-}


case class AddInstance(
  modelDefinitionId: Int,
  source: Source,
  name: Option[String],
  comment: Option[String],
  concurrencyLevel: Int,
  repartitionSource: Option[Int],
  cacheSource: Boolean
)
  extends Script with SourceTransformation
  with PostgresModelMatrixCatalog
  with CliSparkContext
  with Transformers
  with TransformationProcess {

  private val log = LoggerFactory.getLogger(classOf[AddInstance])

  private implicit lazy val sqlContext = ModelMatrix.hiveContext(sc)

  import com.collective.modelmatrix.cli.ASCIITableFormat._
  import com.collective.modelmatrix.cli.ASCIITableFormats._

  def run(): Unit = {

    log.info(s"Add Model Matrix instance. " +
      s"Model definition id: $modelDefinitionId. " +
      s"Source: $source. " +
      s"Name: $name. " +
      s"Comment: $comment. " +
      s"Concurrency: $concurrencyLevel")

    val features = blockOn(db.run(modelDefinitionFeatures.features(modelDefinitionId)))
      .filter(_.feature.active == true)

    require(features.nonEmpty, s"No active features are defined for model definition: $modelDefinitionId. " +
      s"Ensure that this model definition exists")

    val df = toDataFrame(source)

    Transformer.extractFeatures(df, features.map(_.feature)) match {
      // One of extract expressions failed
      case -\/(extractionErrors) =>
        Console.out.println(s"Source feature extraction failed:")
        extractionErrors.printASCIITable()

      // Features extracted, time to transform them!
      case \/-(extracted) =>
        implicit val transformers = new Transformers(extracted)

        val validate = features.map(mdf => mdf -> transformers.validate(mdf.feature))

        val featureErrors = validate.collect { case (mdf, -\/(error)) => mdf -> error }
        val typedFeatures = validate.collect { case (mdf, \/-(typed)) => mdf -> typed }

        // Oops, some schema problem
        if (featureErrors.nonEmpty) {
          Console.err.println(s"Can't create model instance for definition id: $modelDefinitionId from input: $source")
          Console.err.println(s"Feature transformation errors:")
          featureErrors.printASCIITable()
        }

        // Let's run transform!
        if (typedFeatures.nonEmpty) {

          def onError(err: Throwable) = {
            Console.err.println(s"Can't compute input data transformation. Error: $err")
            err.printStackTrace(Console.err)
            throw err
          }

          val modelMatrixTransformation = computeModelMatrixTransformation(typedFeatures, concurrencyLevel)
            .attemptRun.fold(onError, identity)
          val modelInstanceId = modelMatrixTransformation.save(name, comment)

          Console.out.println(s"Successfully created new model instance")
          Console.out.println(s"Matrix Model instance id: $modelInstanceId")
        }
    }
  }
}
