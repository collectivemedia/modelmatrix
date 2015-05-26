package com.collective.modelmatrix.cli.instance

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import com.bethecoder.ascii_table.{ASCIITable, ASCIITableHeader}
import com.collective.modelmatrix.catalog.{ModelDefinitionFeature, ModelMatrixCatalog}
import com.collective.modelmatrix.cli.{CliModelCatalog, CliSparkContext, Script, Source, _}
import com.collective.modelmatrix.transform._
import com.collective.modelmatrix.{CategorialColumn, ModelFeature}
import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataType
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext
import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.{-\/, @@, Tag}


case class Create(
  modelDefinitionId: Int,
  source: Source,
  name: Option[String],
  comment: Option[String],
  concurrencyLevel: Int,
  dbName: String,
  dbConfig: Config
)(implicit val ec: ExecutionContext @@ ModelMatrixCatalog) extends Script with CliModelCatalog with CliSparkContext {

  private implicit val unwrap = Tag.unwrap(ec)

  private object Transformers {
    implicit val sqlContext = new SQLContext(sc)
    val input = source.asDataFrame

    val identity = new IdentityTransformer(input)
    val top = new TopTransformer(input)
    val index = new IndexTransformer(input)
  }

  // Add instance feature operations
  sealed trait AddInstanceFeature

  case class AddIdentityFeature(
    extractType: DataType
  ) extends AddInstanceFeature

  case class AddTopFeature(
    extractType: DataType,
    columns: Seq[CategorialColumn]
  ) extends AddInstanceFeature

  case class AddIndexFeature(
    extractType: DataType,
    columns: Seq[CategorialColumn]
  ) extends AddInstanceFeature

  def run(): Unit = {
    val features = blockOn(db.run(modelDefinitionFeatures.features(modelDefinitionId))).filter(_.feature.active == true)
    require(features.nonEmpty, s"No active features are defined for model definition: $modelDefinitionId. " +
      s"Ensure that this model definition exists")

    val validate = features.map { case mdf@ModelDefinitionFeature(_, _, feature) =>
      mdf -> (Transformers.identity.validate orElse Transformers.top.validate orElse Transformers.index.validate)(feature)
    }

    // Check that input schema match with model definition
    val invalidFeatures = validate.collect { case (mdf, -\/(error)) => mdf -> error }
    if (invalidFeatures.nonEmpty) {
      printInputSchemaErrors(invalidFeatures)
      return
    }

    // Get transformation for each feature
    val typedFeatures: Process[Task, (ModelDefinitionFeature, TypedModelFeature)] =
      Process.emitAll(validate.map { case (mdf, e) => (mdf, e.toOption.get) })

    def onError(err: Throwable) = {
      Console.err.println(s"Can't compute input data transformation. Error: $err")
      err.printStackTrace(Console.err)
      throw err
    }

    // Run with given concurrency level
    val transformed = typedFeatures.concurrently(concurrencyLevel)(transformerChannel).runLog.attemptRun.fold(onError, identity)

    // Create model instance

    val addModelInstance = modelInstances.add(
      name = name,
      modelDefinitionId = modelDefinitionId,
      createdBy = System.getProperty("user.name"),
      createdAt = Instant.now(),
      comment = comment
    )

    val columnId: AtomicInteger = new AtomicInteger(0)
    val insert = for {
      modelInstanceId <- addModelInstance
      featureId <- DBIO.sequence(transformed.map {
        case (ModelDefinitionFeature(featureDefinitionId, _, _), AddIdentityFeature(extractType)) =>
          modelInstanceFeatures.addIdentityFeature(modelInstanceId, featureDefinitionId, extractType, columnId.incrementAndGet())

        case (ModelDefinitionFeature(featureDefinitionId, _, _), AddTopFeature(extractType, columns)) =>
          val baseColumnId = columnId.getAndIncrement()
          val rebased = columns.map(_.rebaseColumnId(baseColumnId))
          modelInstanceFeatures.addTopFeature(modelInstanceId, featureDefinitionId, extractType, rebased)

        case (ModelDefinitionFeature(featureDefinitionId, _, _), AddIndexFeature(extractType, columns)) =>
          val baseColumnId = columnId.getAndIncrement()
          val rebased = columns.map(_.rebaseColumnId(baseColumnId))
          modelInstanceFeatures.addIndexFeature(modelInstanceId, featureDefinitionId, extractType, rebased)
      })
    } yield (modelInstanceId, featureId)

    val (modelInstanceId, featuresId) = blockOn(db.run(insert))
    Console.out.println(s"Successfully created new model instance")
    Console.out.println(s"Matrix Model instance id: $modelInstanceId")
    Console.out.println(s"Matrix Model instance features count: ${featuresId.length}")
  }

  private type In = (ModelDefinitionFeature, TypedModelFeature)
  private type Out = (ModelDefinitionFeature, AddInstanceFeature)

  private val transformerChannel: Channel[Task, In, Out] = channel.lift[Task, In, Out] {
    case (mdf, TypedModelFeature(ModelFeature(_, _, _, _, Identity), extractType)) =>
      Task.now(mdf -> AddIdentityFeature(extractType))

    case (mdf, typed@TypedModelFeature(ModelFeature(_, _, _, _, top: Top), extractType)) =>
      Task.apply(mdf -> AddTopFeature(extractType, Transformers.top.transform(typed)))

    case (mdf, typed@TypedModelFeature(ModelFeature(_, _, _, _, index: Index), extractType)) =>
      Task.apply(mdf -> AddTopFeature(extractType, Transformers.index.transform(typed)))
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

    Console.err.println(s"Can't create model instance for definition id: $modelDefinitionId from input: $source")
    Console.err.println(s"Input schema errors:")
    ASCIITable.getInstance().printTable(inputSchemaErrorHeader, inputSchemaErrorCols.toArray)
  }
}
