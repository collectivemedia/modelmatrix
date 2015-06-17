package com.collective.modelmatrix

import com.collective.modelmatrix.catalog.ModelInstanceFeature
import com.collective.modelmatrix.transform.Transformer
import com.collective.modelmatrix.transform.Transformer.FeatureExtractionError
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import scalaz.{-\/, \/-}

class ModelMatrixFeatureExtractionException(errors: Seq[FeatureExtractionError])
  extends RuntimeException(s"Failed extract model features: [${errors.map(e => e.feature.feature).mkString(", ")}]")

class ModelMatrixFeaturizationException(errors: Seq[FeaturizationError])
  extends RuntimeException(s"Failed to run featurization. Bad features: [${errors.map(e => e.feature).mkString(", ")}]")

/**
 * Model Matrix client class that hides database interaction and provides
 * higher level API than classes in core
 *
 * @param sc Spark Context
 */
class ModelMatrixClient(sc: SparkContext) extends ClientModelCatalog {
  private val log = LoggerFactory.getLogger(classOf[ModelMatrixClient])

  private implicit val sqlContext = ModelMatrix.hiveContext(sc)

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


}
