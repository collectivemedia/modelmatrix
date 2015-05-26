package com.collective.modelmatrix.cli.instance

import com.bethecoder.ascii_table.{ASCIITable, ASCIITableHeader}
import com.collective.modelmatrix.catalog.{ModelDefinitionFeature, ModelMatrixCatalog}
import com.collective.modelmatrix.cli.{Source, _}
import com.collective.modelmatrix.transform._
import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext

import scala.concurrent.ExecutionContext
import scalaz._

case class Validate(
  modelDefinitionId: Int,
  source: Source,
  dbName: String,
  dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog with CliSparkContext {

  private object Transformers {
    implicit val sqlContext = new SQLContext(sc)
    val input = source.asDataFrame

    val identity = new IdentityTransformer(input)
    val top = new TopTransformer(input)
    val index = new IndexTransformer(input)
  }

  def run(): Unit = {
    val features = blockOn(db.run(modelDefinitionFeatures.features(modelDefinitionId))).filter(_.feature.active == true)
    require(features.nonEmpty, s"No active features are defined for model definition: $modelDefinitionId. " +
      s"Ensure that this model definition exists")

    val validate = features.map { case mdf@ModelDefinitionFeature(_, _, feature) =>
      mdf -> (Transformers.identity.validate orElse Transformers.top.validate orElse Transformers.index.validate)(feature)
    }

    // Print schema errors
    val invalidFeatures = validate.collect { case (mdf, -\/(error)) => mdf -> error }
    if (invalidFeatures.nonEmpty) {
      printInputSchemaErrors(invalidFeatures)
    }

    // Print schema typed features
    val typedFeatures = validate.collect { case (mdf, \/-(typed)) => mdf -> typed }
    if (typedFeatures.nonEmpty) {
      printInputTypedFeatures(typedFeatures)
    }
  }

  private def printInputTypedFeatures(invalidFeatures: Seq[(ModelDefinitionFeature, TypedModelFeature)]): Unit = {
    val inputTypedHeader: Array[ASCIITableHeader] = Array(
      "Id", "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters", "Extract Type"
    )
    val inputTypedCols: Seq[Array[String]] = invalidFeatures.map {
      case (ModelDefinitionFeature(id, _, feature), typed) =>
        Array(
          id.toString,
          feature.active.toString,
          feature.group,
          feature.feature,
          feature.extract,
          feature.transform.stringify,
          printParameters(feature.transform),
          typed.extractType.toString
        )
    }

    Console.out.println(s"Input schema typed features:")
    ASCIITable.getInstance().printTable(inputTypedHeader, inputTypedCols.toArray)
  }

  private def printInputSchemaErrors(invalidFeatures: Seq[(ModelDefinitionFeature, InputSchemaError)]): Unit = {
    val inputSchemaErrorHeader: Array[ASCIITableHeader] = Array(
      "Id", "Active", "Group", "Feature", "Extract", "Transform", "Transform Parameters", "Error"
    )
    val inputSchemaErrorCols: Seq[Array[String]] = invalidFeatures.map {
      case (ModelDefinitionFeature(id, _, feature), error) =>
        Array(
          id.toString,
          feature.active.toString,
          feature.group,
          feature.feature,
          feature.extract,
          feature.transform.stringify,
          printParameters(feature.transform),
          error.errorMessage
        )
    }

    Console.out.println(s"Input schema errors:")
    ASCIITable.getInstance().printTable(inputSchemaErrorHeader, inputSchemaErrorCols.toArray)
  }

}
