package com.collective.modelmatrix

import java.time.Instant
import java.util.concurrent.Executors

import com.collective.modelmatrix.catalog.{ModelDefinitionFeature, ModelInstanceFeature}
import com.collective.modelmatrix.transform._
import com.collective.modelmatrix.transform.Transformer.FeatureExtractionError
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext
import scalaz.{-\/, \/-}

class ModelMatrixFeatureExtractionException(errors: Seq[FeatureExtractionError])
  extends RuntimeException(s"Failed extract model features: [${errors.map(e => e.feature.feature).mkString(", ")}]")

class ModelMatrixFeaturizationException(errors: Seq[FeaturizationError])
  extends RuntimeException(s"Failed to run featurization. Bad features: [${errors.map(e => e.feature).mkString(", ")}]")

class ModelMatrixFeatureTransformationException(errors: Seq[(ModelDefinitionFeature, FeatureTransformationError)])
  extends RuntimeException(s"Failed to run transformation. Bad features [${errors.map(_._1.feature).mkString(", ")}]")

/**
 * Model Matrix client class that hides database interaction and provides
 * higher level API than classes in core
 *
 * @param sc Spark Context
 */
class ModelMatrixClient(sc: SparkContext) extends ClientModelCatalog with Transformers {
  private val log = LoggerFactory.getLogger(classOf[ModelMatrixClient])

  private implicit val sqlContext = ModelMatrix.hiveContext(sc)

  // Implicit Execution context for Database operations
  private implicit val catalogExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10, threadFactory("catalog-db-pool", daemon = true)))

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
  def featurize[L](modelInstanceId: Int, df: DataFrame, labeling: Labeling[L]): RDD[(L, Vector)] = {

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
  def featurize[L](features: Seq[ModelInstanceFeature], df: DataFrame, labeling: Labeling[L]): RDD[(L, Vector)] = {

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
   * Create Model Matrix instance
   *
   * @param modelDefinitionId model matrix definition id
   * @param df                data frame
   * @param name              instance id
   * @param comment           instance comment
   * @param concurrencyLevel  number of concurrent transformation calculations
   * @return
   */
  def createModelMatrixInstance(
    modelDefinitionId: Int,
    df: DataFrame,
    name: Option[String],
    comment: Option[String],
    concurrencyLevel: Int
  ): Int = {

    log.info(s"Create new Model Matrix instance for definition: $modelDefinitionId. " +
      s"Name: $name. Comment: $comment. Concurrency: $concurrencyLevel")

    val features = blockOn(db.run(modelDefinitionFeatures.features(modelDefinitionId)))
      .filter(_.feature.active == true)

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

        require(typedFeatures.nonEmpty, s"Empty feature set for transformation")

        runTransform(typedFeatures, modelDefinitionId, name, comment, concurrencyLevel)
    }

  }

  // TODO: It's noticeable code duplication with AddInstance in cli
  // TODO: But I don't want to put this code into core

  import scalaz.concurrent.Task
  import scalaz.stream._

  sealed trait InstanceCommand

  case class AddIdentityFeature(extractType: DataType) extends InstanceCommand
  case class AddTopFeature(extractType: DataType, columns: Seq[CategorialColumn]) extends InstanceCommand
  case class AddIndexFeature(extractType: DataType, columns: Seq[CategorialColumn]) extends InstanceCommand
  case class AddBinsFeature(extractType: DataType, columns: Seq[BinColumn]) extends InstanceCommand

  private def runTransform(
    features: Seq[(ModelDefinitionFeature, TypedModelFeature)],
    modelDefinitionId: Int,
    name: Option[String],
    comment: Option[String],
    concurrencyLevel: Int
  )(implicit t: Transformers): Int = {

    // Get transformation for each feature
    val typedFeatures: Process[Task, (ModelDefinitionFeature, TypedModelFeature)] =
      Process.emitAll(features)

    def onError(err: Throwable) = throw err

    // Run with given concurrency level
    val commands = typedFeatures
      .concurrently(concurrencyLevel)(prepareInstanceCommand)
      .runLog.attemptRun.fold(onError, identity)

    // Create model instance
    val addModelInstance = modelInstances.add(
      name = name,
      modelDefinitionId = modelDefinitionId,
      createdBy = System.getProperty("user.name"),
      createdAt = Instant.now(),
      comment = comment
    )

    var columnId: Int = 0

    val insert = for {
      modelInstanceId <- addModelInstance
      featureId <- DBIO.sequence(commands.map {
        case (ModelDefinitionFeature(featureDefinitionId, _, _), AddIdentityFeature(extractType)) =>
          columnId += 1
          modelInstanceFeatures.addIdentityFeature(modelInstanceId, featureDefinitionId, extractType, columnId)

        case (ModelDefinitionFeature(featureDefinitionId, _, _), AddTopFeature(extractType, columns)) =>
          val baseColumnId = columnId
          columnId += columns.size
          val rebased = columns.map(_.rebaseColumnId(baseColumnId))
          modelInstanceFeatures.addTopFeature(modelInstanceId, featureDefinitionId, extractType, rebased)

        case (ModelDefinitionFeature(featureDefinitionId, _, _), AddIndexFeature(extractType, columns)) =>
          val baseColumnId = columnId
          columnId += columns.size
          val rebased = columns.map(_.rebaseColumnId(baseColumnId))
          modelInstanceFeatures.addIndexFeature(modelInstanceId, featureDefinitionId, extractType, rebased)

        case (ModelDefinitionFeature(featureDefinitionId, _, _), AddBinsFeature(extractType, columns)) =>
          val baseColumnId = columnId
          columnId += columns.size
          val rebased = columns.map(_.rebaseColumnId(baseColumnId))
          modelInstanceFeatures.addBinsFeature(modelInstanceId, featureDefinitionId, extractType, rebased)
      })
    } yield (modelInstanceId, featureId)

    import driver.api._
    val (modelInstanceId, _) = blockOn(db.run(insert.transactionally))

    log.debug(s"Successfully created new model instance: $modelInstanceId")

    modelInstanceId
  }

  private type In = (ModelDefinitionFeature, TypedModelFeature)
  private type Out = (ModelDefinitionFeature, InstanceCommand)

  private def prepareInstanceCommand(implicit transformers: Transformers): Channel[Task, In, Out] = channel.lift[Task, In, Out] {
    case (mdf, TypedModelFeature(ModelFeature(_, _, _, _, Identity), extractType)) =>
      Task.now(mdf -> AddIdentityFeature(extractType))

    case (mdf, typed@TypedModelFeature(ModelFeature(_, _, _, _, top: Top), extractType)) =>
      Task.apply(mdf -> AddTopFeature(extractType, transformers.top.transform(typed)))

    case (mdf, typed@TypedModelFeature(ModelFeature(_, _, _, _, index: Index), extractType)) =>
      Task.apply(mdf -> AddIndexFeature(extractType, transformers.index.transform(typed)))

    case (mdf, typed@TypedModelFeature(ModelFeature(_, _, _, _, bins: Bins), extractType)) =>
      Task.apply(mdf -> AddBinsFeature(extractType, transformers.bins.transform(typed)))
  }

  private implicit class ConcurrentProcess[O](val process: Process[Task, O]) {
    def concurrently[O2](concurrencyLevel: Int)
        (f: Channel[Task, O, O2]): Process[Task, O2] = {
      val actions =
        process.
          zipWith(f)((data, f) => f(data))

      val nestedActions =
        actions.map(Process.eval)

      merge.mergeN(concurrencyLevel)(nestedActions)
    }
  }

  private def threadFactory(prefix: String, daemon: Boolean) =
    new ThreadFactoryBuilder().
      setDaemon(daemon).
      setNameFormat(s"$prefix-%d").
      build()

}
