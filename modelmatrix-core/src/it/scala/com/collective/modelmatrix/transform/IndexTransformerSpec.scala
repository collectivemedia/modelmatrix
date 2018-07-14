package com.collective.modelmatrix.transform

import com.collective.modelmatrix.CategoricalColumn.{AllOther, CategoricalValue}
import com.collective.modelmatrix.{ModelFeature, ModelMatrix, ModelMatrixEncoding, TestSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FlatSpec

import scala.util.Random
import scalaz.syntax.either._
import scalaz.{-\/, \/-}

class IndexTransformerSpec extends FlatSpec with TestSparkContext {

  val schema = StructType(Seq(
    StructField("adv_site", StringType)
  ))

  val input = new Random().shuffle(
    Seq.fill(5000)(Row("cnn.com")) ++
      Seq.fill(4000)(Row("bbc.com")) ++
      Seq.fill(400)(Row("hbo.com")) ++
      Seq.fill(200)(Row("mashable.com")) ++
      // first 4 sites supports more than 2% of data set
      Seq.fill(100)(Row("gizmodo.com")) ++
      Seq.fill(100)(Row("reddit.com")) ++
      Seq.fill(100)(Row("amc.com")) ++
      Seq.fill(100)(Row("msnbc.com")) ++
      // null columns should be skipped
      Seq.fill(100)(Row(null))
  )

  val isActive = true
  val withAllOther = true

  val adSite = ModelFeature(isActive, "Ad", "ad_site", "adv_site", Index(2, withAllOther))

  val df = sqlContext.createDataFrame(sc.parallelize(input), schema)
  val transformer = new IndexTransformer(Transformer.extractFeatures(df, Seq(adSite)) match {
    case -\/(err) => sys.error(s"Can't extract features: $err")
    case \/-(suc) => suc
  })

  "Index Transformer" should "support string typed model feature" in {
    val valid = transformer.validate(adSite)
    assert(valid == TypedModelFeature(adSite, StringType).right)
  }

  it should "fail if feature column doesn't exists" in {
    val failed = transformer.validate(adSite.copy(feature = "adv_site"))
    assert(failed == FeatureTransformationError.FeatureColumnNotFound("adv_site").left)
  }

  it should "calculate correct categorical columns with all other" in {
    val typed = transformer.validate(adSite).toOption.get
    val columns = transformer.transform(typed)

    assert(columns.size == 4)

    assert(columns(0).columnId == 1)
    assert(columns(0).isInstanceOf[CategoricalValue])
    assert(columns(0).asInstanceOf[CategoricalValue].sourceName == "cnn.com")
    assert(columns(0).asInstanceOf[CategoricalValue].sourceValue == ModelMatrixEncoding.encode("cnn.com"))
    assert(columns(0).asInstanceOf[CategoricalValue].count == 5000)
    assert(columns(0).asInstanceOf[CategoricalValue].cumulativeCount == 5000)

    assert(columns(1).columnId == 2)
    assert(columns(1).isInstanceOf[CategoricalValue])
    assert(columns(1).asInstanceOf[CategoricalValue].sourceName == "bbc.com")
    assert(columns(1).asInstanceOf[CategoricalValue].sourceValue == ModelMatrixEncoding.encode("bbc.com"))
    assert(columns(1).asInstanceOf[CategoricalValue].count == 4000)
    assert(columns(1).asInstanceOf[CategoricalValue].cumulativeCount == 9000)

    assert(columns(2).columnId == 3)
    assert(columns(2).isInstanceOf[CategoricalValue])
    assert(columns(2).asInstanceOf[CategoricalValue].sourceName == "hbo.com")
    assert(columns(2).asInstanceOf[CategoricalValue].sourceValue == ModelMatrixEncoding.encode("hbo.com"))
    assert(columns(2).asInstanceOf[CategoricalValue].count == 400)
    assert(columns(2).asInstanceOf[CategoricalValue].cumulativeCount == 9400)

    assert(columns(3).columnId == 4)
    assert(columns(3).isInstanceOf[AllOther])
    assert(columns(3).asInstanceOf[AllOther].count == 600)
    assert(columns(3).asInstanceOf[AllOther].cumulativeCount == 10000)
  }

  it should "remove all other column" in {
    val typed = transformer.validate(adSite.copy(transform = Index(2.0, allOther = false))).toOption.get
    val columns = transformer.transform(typed)

    assert(columns.size == 3)
  }

  it should "return less columns with higher support factor" in {
    val typed = transformer.validate(adSite.copy(transform = Index(10.0, allOther = false))).toOption.get
    val columns = transformer.transform(typed)
    assert(columns.size == 2)
  }

  it should "return all columns with low support factor" in {
    val typed = transformer.validate(adSite.copy(transform = Index(0.0001, withAllOther))).toOption.get
    val columns = transformer.transform(typed)
    assert(columns.size == 8)
  }

}
