package com.collective.modelmatrix

import java.time.Instant

import com.collective.modelmatrix.ModelMatrix.ModelMatrixCatalogAccess
import com.collective.modelmatrix.catalog.ModelDefinitionFeature
import com.collective.modelmatrix.transform._
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory
import slick.dbio.DBIO

import scalaz.Tag

/**
 * Calculate Model Matrix transformation concurrently using scalaz-stream Process
 */
trait TransformationProcess {
  self: ModelMatrixCatalogAccess with Transformers =>

  private val log = LoggerFactory.getLogger(classOf[TransformationProcess])

  implicit val ec = Tag.unwrap(catalogExecutionContext)

  import scalaz.concurrent.Task
  import scalaz.stream._

  trait ModelMatrixTransformation {
    def save(name: Option[String], comment: Option[String]): Int
  }

  private sealed trait AddFeature

  private object AddFeature {
    case class AddIdentityFeature(extractType: DataType) extends AddFeature
    case class AddTopFeature(extractType: DataType, columns: Seq[CategorialColumn]) extends AddFeature
    case class AddIndexFeature(extractType: DataType, columns: Seq[CategorialColumn]) extends AddFeature
    case class AddBinsFeature(extractType: DataType, columns: Seq[BinColumn]) extends AddFeature
  }

  import AddFeature._

  protected def computeModelMatrixTransformation(
    features: Seq[(ModelDefinitionFeature, TypedModelFeature)],
    concurrencyLevel: Int
  )(implicit t: Transformers): Task[ModelMatrixTransformation] = {

    require(features.nonEmpty, s"Can't compute model matrix transformation for empty features")

    // All features should belong to the same model definition
    val modelDefinitionIds = features.map(_._1.modelDefinitionId).toSet
    require(modelDefinitionIds.size == 1,
      s"Model features belong to multiple model definitions: [${modelDefinitionIds.mkString(", ")}}]")

    // Start with scalaz-stream Process
    val typedFeatures: Process[Task, (ModelDefinitionFeature, TypedModelFeature)] =
      Process.emitAll(features)

    // Run with given concurrency level
    val process = typedFeatures
      .concurrently(concurrencyLevel)(prepareInstanceCommand)
      .runLog

    process map { commands => new ModelMatrixTransformation {
      def save(name: Option[String], comment: Option[String]): Int = {
        log.info(s"Save model matrix transformations with name: ${name.getOrElse("")} and comment: ${comment.getOrElse("")}")

        val addModelInstance = modelInstances.add(
          name = name,
          modelDefinitionId = modelDefinitionIds.head,
          createdBy = System.getProperty("user.name"),
          createdAt = Instant.now(),
          comment = comment
        )

        var columnId: Int = 0

        val insert = for {
          modelInstanceId <- addModelInstance
          featureId <- DBIO.sequence(commands.sortBy(_._1.id /* save features ordered by definition id */).map {
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
    }
    }
  }

  private type In = (ModelDefinitionFeature, TypedModelFeature)
  private type Out = (ModelDefinitionFeature, AddFeature)

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

}
