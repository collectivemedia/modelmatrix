package com.collective.modelmatrix

import java.nio.file.{Path, Paths}
import java.time.Instant
import java.util.concurrent.Executors

import com.collective.modelmatrix.ModelMatrix.ModelMatrixCatalogAccess
import com.collective.modelmatrix.catalog.{ModelDefinitionFeature, ModelInstanceFeature, _}
import com.collective.modelmatrix.db.DefaultDatabaseConfig
import com.collective.modelmatrix.transform.Transformer.FeatureExtractionError
import com.collective.modelmatrix.transform._
import org.spark_project.guava.util.concurrent.ThreadFactoryBuilder
import com.typesafe.config.{ConfigFactory, ConfigResolveOptions}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe.{TypeTag}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scalaz.{-\/, \/-, _}


class ModelMatrixDefinitionException(errors: Seq[(String, NonEmptyList[String])])
  extends RuntimeException(s"Failed to create model matrix definition: [${errors.mkString(", ")}]")

class ModelMatrixFeatureExtractionException(errors: Seq[FeatureExtractionError])
  extends RuntimeException(s"Failed extract model features: [${errors.map(e => e.feature.feature).mkString(", ")}]")

class ModelMatrixFeaturizationException(errors: Seq[FeaturizationError])
  extends RuntimeException(s"Failed to run featurization. Bad features: [${errors.map(e => e.feature).mkString(", ")}]")

class ModelMatrixFeatureTransformationException(errors: Seq[(ModelDefinitionFeature, FeatureTransformationError)])
  extends RuntimeException(s"Failed to run transformation. Bad features [${errors.map(_._1.feature).mkString(", ")}]")

object ModelMatrix extends ModelMatrixUDF {

//  def sqlContext(session: SparkSession): SQLContext = {
//    // val sqlContext = new SQLContext(sc)
//    registerUDF(session.udf)
//    session.sqlContext
//  }

//  def hiveContext(session: SparkSession): HiveContext = {
//    //val sqlContext = new HiveContext(sc)
//    registerUDF(session.udf)  // #todo
//    session.sqlContext
//  }


  trait ModelMatrixCatalogAccess {
    protected val driver = DefaultDatabaseConfig.slickDriver

    protected lazy val db = DefaultDatabaseConfig.database()
    protected lazy val catalog = new ModelMatrixCatalog(driver)

    protected lazy val modelDefinitions = new ModelDefinitions(catalog)
    protected lazy val modelDefinitionFeatures = new ModelDefinitionFeatures(catalog)

    protected lazy val modelInstances = new ModelInstances(catalog)
    protected lazy val modelInstanceFeatures = new ModelInstanceFeatures(catalog)

    protected def blockOn[T](f: Future[T], duration: FiniteDuration = 10.seconds) = {
      Await.result(f, duration)
    }

    protected implicit val catalogExecutionContext: ExecutionContext @@ ModelMatrixCatalog =
      Tag[ExecutionContext, ModelMatrixCatalog](ExecutionContext.fromExecutor(
        Executors.newFixedThreadPool(10, threadFactory("modelmatrix-catalog-db-pool", daemon = true)))
      )

    private def threadFactory(prefix: String, daemon: Boolean) =
      new ThreadFactoryBuilder().
        setDaemon(daemon).
        setNameFormat(s"$prefix-%d").
        build()
  }
}

/**
 * Model Matrix class that hides database interaction and provides
 * higher level API
 *
 */
class ModelMatrix(session: SparkSession) extends ModelMatrixCatalogAccess with Transformers with TransformationProcess {
  private val log = LoggerFactory.getLogger(classOf[ModelMatrix])

  // def this(sc: SparkContext) = this(ModelMatrix.hiveContext(sc)) #todo

  /**
   * Get Model Matrix instance transformations for given model instance id
   *
   * @param modelInstanceId model instance id
   * @return model instance features
   */
  def instanceTransformations(modelInstanceId: Int): Seq[ModelInstanceFeature] = {
    blockOn(db.run(modelInstanceFeatures.features(modelInstanceId)))
  }

  /**
   * Apply model instance transformations to data frame
   *
   * @param modelInstanceId model instance id
   * @param df data frame
   * @param labeling row labeling
   * @return RDD of featurized vectors
   */
  def featurize[L](modelInstanceId: Int, df: DataFrame, labeling: Labeling[L])
                  (implicit labelEncoder : Encoder[L], tagL : TypeTag[(L, Vector)])
    : Dataset[(L, Vector)] = {

    log.info(s"Featurization data frame using Model Matrix instance: $modelInstanceId")

    val features = blockOn(db.run(modelInstanceFeatures.features(modelInstanceId)))
    require(features.nonEmpty, s"No features are defined for model instance: $modelInstanceId. " +
      s"Ensure that this model instance exists")

    featurize(features, df, labeling)
  }

  /**
   * Apply model instance transformations to data frame
   *
   * @param features model instance features
   * @param df data frame
   * @param labeling row labeling
   * @return RDD of featurized vectors
   */
  def featurize[L](features: Seq[ModelInstanceFeature], df: DataFrame, labeling: Labeling[L])
                  (implicit labelEncoder: Encoder[L], tagL : TypeTag[(L, Vector)])
    : Dataset[(L, Vector)] = {

    require(features.nonEmpty, s"Can't do featurization without features")

    val featurization = new Featurization(features)

    Transformer.extractFeatures(df, features.map(_.feature), labeling) match {
      // Feature extraction failed
      case -\/(extractionErrors) =>
        extractionErrors.foreach { err =>
          log.error(s"Feature extraction error: ${err.feature.feature}. Error: ${err.error.getMessage}")
        }
        throw new ModelMatrixFeatureExtractionException(extractionErrors)

      // Featurization schema is not compatible
      case \/-(extracted) if featurization.validateLabeled(extracted).exists(_.isLeft) =>
        val errors = featurization.validateLabeled(extracted).collect { case -\/(err) => err }
        errors.foreach { err =>
          log.error(s"Featurization error: ${err.errorMessage}")
        }
        throw new ModelMatrixFeaturizationException(errors)

      // All good, let's do featurization
      case \/-(extracted) => featurization.featurize(extracted, labeling)
    }
  }


  /**
   * Create the model matrix definition based on configuration file.
   * If model definition already exists for passed checksum return existing id
   *
   * @param modelDefinitionFilePath location of the model definition configuration file
   * @param name                    name of the model definition
   * @param comment                 additional comment for the model definition
   * @return
   */
  def createModelMatrixDefinition(
    modelDefinitionFilePath: String,
    name: Option[String],
    comment: Option[String]
    ): Int = {

    val modelDefinitionFile: Path = Paths.get(modelDefinitionFilePath)

    val parser = new ModelConfigurationParser(
      ConfigFactory.parseFile(modelDefinitionFile.toFile).resolve(ConfigResolveOptions.defaults())
    )

    val features = parser.features()

    val errors = features collect { case (f, Failure(e)) => (f, e) }
    val success = features collect { case (_, Success(feature)) => feature }

    // If there are errors in the model feature definition file throw an exception
    if (errors.nonEmpty) {
      log.error(s"Incorrect configured model features: ${errors.size}")
      throw new ModelMatrixDefinitionException(errors)
    }

    if (success.nonEmpty && errors.isEmpty) {

      blockOn(db.run(modelDefinitions.findByContent(parser.content))) match {
        case Some(definition) =>
          log.debug(s"Model matrix definition already exists. Id: ${definition.id}")
          definition.id

        case None =>
          val insert = for {
            id <- modelDefinitions.add(
              name = name,
              source = parser.content,
              createdBy = System.getProperty("user.name"),
              createdAt = Instant.now(),
              comment = comment
            )
            featureId <- modelDefinitionFeatures.addFeatures(id, success: _*)
          } yield (id, featureId)

          import driver.api._
          val (modelDefinitionId, _) = blockOn(db.run(insert.transactionally))

          log.info(s"Created Model Matrix definition. Id: $modelDefinitionId")
          modelDefinitionId
      }
    } else {
      sys.error(s"Configuration doesn't contain any feature definition")
    }
  }


  /**
   * Create Model Matrix instance
   *
   * @param modelDefinitionId model matrix definition id
   * @param df                data frame
   * @param name              instance id
   * @param comment           instance comment
   * @param concurrencyLevel  number of concurrent transformation calculations
   * @return                  model matrix instance id
   */
  def createModelMatrixInstance(
    modelDefinitionId: Int,
    df: DataFrame,
    name: Option[String],
    comment: Option[String],
    concurrencyLevel: Int
  )(implicit session: SparkSession): Int = {

    log.info(s"Create new Model Matrix instance for definition: $modelDefinitionId. " +
      s"Name: $name. Comment: $comment. Concurrency: $concurrencyLevel")

    val features = blockOn(db.run(modelDefinitionFeatures.features(modelDefinitionId)))
      .filter(_.feature.active == true)

    log.debug(s"Found ${features.size} model features for definition: $modelDefinitionId")

    require(features.nonEmpty, s"No active features are defined for model definition: $modelDefinitionId. " +
      s"Ensure that this model definition exists")

    Transformer.extractFeatures(df, features.map(_.feature)) match {
      // Feature extraction failed
      case -\/(extractionErrors) =>
        extractionErrors.foreach { err =>
          log.error(s"Feature extraction error: ${err.feature.feature}. Error: ${err.error.getMessage}")
        }
        throw new ModelMatrixFeatureExtractionException(extractionErrors)

      // Features extracted, time to transform them!
      case \/-(extracted) =>
        implicit val transformers = new Transformers(extracted)

        val validate = features.map(mdf => mdf -> transformers.validate(mdf.feature))

        val featureErrors = validate.collect { case (mdf, -\/(error)) => mdf -> error }
        val typedFeatures = validate.collect { case (mdf, \/-(typed)) => mdf -> typed }

        // Oops, some schema problem
        if (featureErrors.nonEmpty) {
          featureErrors.foreach { err =>
            log.error(s"Feature transformation error: ${err._1.feature}. Error: ${err._2.errorMessage}")
          }
          throw new ModelMatrixFeatureTransformationException(featureErrors)
        }

        val modelMatrixTransformation = computeModelMatrixTransformation(typedFeatures, concurrencyLevel).run
        modelMatrixTransformation.save(name, comment)
    }
  }

}
