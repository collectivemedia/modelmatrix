package com.collective.modelmatrix.catalog

import java.time.Instant

import slick.driver.JdbcProfile


class ModelMatrixCatalog(private[catalog] val driver: JdbcProfile)
  extends ModelMatrixDefinition with ModelMatrixInstance {

  import driver.api._

  // scalastyle:off

  private[catalog] object tables {
    val AutoIncId = -1

    val modelDefinitions = TableQuery[mmc_definition]
    val featureDefinitions = TableQuery[mmc_definition_feature]
    val topParameters = TableQuery[mmc_definition_feature_top_param]
    val indexParameters = TableQuery[mmc_definition_feature_index_param]

    val modelInstances = TableQuery[mmc_instance]
    val featureInstances = TableQuery[mmc_instance_feature]
    val topValues = TableQuery[mmc_instance_feature_top_value]
    val indexValues = TableQuery[mmc_instance_feature_index_value]

  }

  // scalastyle:on

  def create: DBIO[Unit] = {
    tables.modelDefinitions.schema.create   >>
    tables.featureDefinitions.schema.create >>
    tables.topParameters.schema.create      >>
    tables.indexParameters.schema.create
  }

}

/**
 * Model Matrix definition tables
 */
trait ModelMatrixDefinition { self: ModelMatrixCatalog =>
  // scalastyle:off

  import self.driver.api._

  // Model Definition
  private[catalog] class mmc_definition(tag: Tag)
    extends Table[(Int, Option[String], String, String, Instant, Option[String])](tag, "mmc_definition") {

    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def name = column[Option[String]]("name")
    def source = column[String]("source")
    def createdBy = column[String]("created_by")
    def createdAt = column[Instant]("created_at")
    def comment = column[Option[String]]("comment")

    def * = (id, name, source, createdBy, createdAt, comment)
  }

  // Model Feature Definition
  private[catalog] class mmc_definition_feature(tag: Tag)
    extends Table[(Int, Int, Boolean, String, String, String, String)](tag, "mmc_definition_feature") {

    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def modelDefinitionId = column[Int]("model_definition_id")
    def active = column[Boolean]("active")
    def group = column[String]("grp")
    def feature = column[String]("feature")
    def extract = column[String]("extrct")
    def transform = column[String]("transform")

    def * = (id, modelDefinitionId, active, group, feature, extract, transform)

    // Foreign kew that can be navigated to crete a join
    def modelDefinition = foreignKey("mmc_definition_feature_fk", modelDefinitionId, tables.modelDefinitions)(_.id)
  }

  // Top transform parameters
  private[catalog] class mmc_definition_feature_top_param(tag: Tag)
    extends Table[(Int, Int, Double, Boolean)](tag, "mmc_definition_feature_top_param") {

    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def featureDefinitionId = column[Int]("feature_definition_id")
    def percentage = column[Double]("percentage")
    def allOther = column[Boolean]("all_other")

    def * = (id, featureDefinitionId, percentage, allOther)

    // Foreign kew that can be navigated to crete a join
    def featureDefinition = foreignKey("mmc_definition_feature_top_param_fk", featureDefinitionId, tables.featureDefinitions)(_.id)
  }

  // Index transform parameters
  private[catalog] class mmc_definition_feature_index_param(tag: Tag)
    extends Table[(Int, Int, Double, Boolean)](tag, "mmc_definition_feature_index_param") {

    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def featureDefinitionId = column[Int]("feature_definition_id")
    def percentage = column[Double]("percentage")
    def allOther = column[Boolean]("all_other")

    def * = (id, featureDefinitionId, percentage, allOther)

    // Foreign kew that can be navigated to crete a join
    def featureDefinition = foreignKey("mmc_definition_feature_index_param_fk", featureDefinitionId, tables.featureDefinitions)(_.id)
  }

  // Type gymnastics
  private[catalog] type modelDefinitionsT = slick.lifted.Query[mmc_definition,(Int, Option[String], String, String, java.time.Instant, Option[String]),Seq]

  // scalastyle:on
}

/**
 * Model Matrix instance tables
 */
trait ModelMatrixInstance { self: ModelMatrixCatalog =>
  // scalastyle:off

  import self.driver.api._

  // Model Instance
  private[catalog] class mmc_instance(tag: Tag)
    extends Table[(Int, Int, Option[String], String, Instant, Option[String])](tag, "mmc_instance") {

    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def modelDefinitionId = column[Int]("model_definition_id")
    def name = column[Option[String]]("name")
    def createdBy = column[String]("created_by")
    def createdAt = column[Instant]("created_at")
    def comment = column[Option[String]]("comment")

    def * = (id, modelDefinitionId, name, createdBy, createdAt, comment)
  }

  // Model Feature Instance
  private[catalog] class mmc_instance_feature(tag: Tag)
    extends Table[(Int, Int, Int, String)](tag, "mmc_instance_feature") {

    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def modelInstanceId = column[Int]("model_instance_id")
    def featureDefinitionId = column[Int]("feature_definition_id")
    def extractType = column[String]("extract_type")

    def * = (id, modelInstanceId, featureDefinitionId, extractType)

    // Foreign kew that can be navigated to crete a join
    def modelInstance = foreignKey("mmc_instance_feature_fk", modelInstanceId, tables.modelInstances)(_.id)
    def featureDefinition = foreignKey("mmc_instance_feature_definition_fk", featureDefinitionId, tables.featureDefinitions)(_.id)
  }

  // Top values
  private[catalog] class mmc_instance_feature_top_value(tag: Tag)
    extends Table[(Int, Int, Int, String, Array[Byte], Long, Long)](tag, "mmc_instance_feature_top_value") {

    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def featureInstanceId = column[Int]("feature_instance_id")
    def columnId = column[Int]("column_id")
    def sourceName = column[String]("source_name")
    def sourceValue = column[Array[Byte]]("source_value")
    def count = column[Long]("cnt")
    def cumulativeCount = column[Long]("cumulative_cnt")

    def * = (id, featureInstanceId, columnId, sourceName, sourceValue, count, cumulativeCount)

    // Foreign kew that can be navigated to crete a join
    def featureDefinition = foreignKey("mmc_instance_feature_top_value_fk", featureInstanceId, tables.featureInstances)(_.id)
  }

  // Index values
  private[catalog] class mmc_instance_feature_index_value(tag: Tag)
    extends Table[(Int, Int, Int, String, Array[Byte], Long, Long)](tag, "mmc_instance_feature_index_value") {

    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def featureInstanceId = column[Int]("feature_instance_id")
    def columnId = column[Int]("column_id")
    def sourceName = column[String]("source_name")
    def sourceValue = column[Array[Byte]]("source_value")
    def count = column[Long]("cnt")
    def cumulativeCount = column[Long]("cumulative_cnt")

    def * = (id, featureInstanceId, columnId, sourceName, sourceValue, count, cumulativeCount)

    // Foreign kew that can be navigated to crete a join
    def featureDefinition = foreignKey("mmc_instance_feature_index_value_fk", featureInstanceId, tables.featureInstances)(_.id)
  }

  // Type gymnastics
  private[catalog] type modelInstancesT = slick.lifted.Query[mmc_instance,(Int, Int, Option[String], String, Instant, Option[String]), Seq]

  // scalastyle:on
}
