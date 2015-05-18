package com.collective.modelmatrix.catalog

import java.time.Instant

import slick.driver.JdbcProfile

class ModelMatrixCatalog(private[catalog] val driver: JdbcProfile) {

  import driver.api._

  // scalastyle:off

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

  // Top transform parameters
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

  private[catalog] object tables {
    val AutoIncId = -1

    val modelDefinitions = TableQuery[mmc_definition]
    val featureDefinitions = TableQuery[mmc_definition_feature]
    val topParameters = TableQuery[mmc_definition_feature_top_param]
    val indexParameters = TableQuery[mmc_definition_feature_index_param]
  }

  // scalastyle:on

  def create: DBIO[Unit] = {
    tables.modelDefinitions.schema.create   >>
    tables.featureDefinitions.schema.create >>
    tables.topParameters.schema.create      >>
    tables.indexParameters.schema.create
  }

}
