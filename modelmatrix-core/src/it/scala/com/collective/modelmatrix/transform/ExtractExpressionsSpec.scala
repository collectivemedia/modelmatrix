package com.collective.modelmatrix.transform

import java.time.temporal.{ChronoField, ChronoUnit}
import java.time.{DayOfWeek, Instant, ZoneOffset}
import java.util.UUID

import com.collective.modelmatrix.CategoricalColumn.CategoricalValue
import com.collective.modelmatrix.{ModelFeature, ModelMatrix, ModelMatrixEncoding, TestSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

import scala.util.Random
import scalaz.syntax.either._
import scalaz.{-\/, \/-}

class ExtractExpressionsSpec extends FlatSpec with TestSparkContext {

  val rnd = new Random()

  val now = Instant.now().atZone(ZoneOffset.UTC)
  val yesterday = now.minus(1, ChronoUnit.DAYS).minus(1, ChronoUnit.HOURS).withZoneSameInstant(ZoneOffset.UTC)

  val DayOfWeekNow = DayOfWeek.from(now).getValue
  val DayOfWeekYesterday = DayOfWeek.from(yesterday).getValue

  val HourOfDayNow = now.get(ChronoField.HOUR_OF_DAY)
  val HourOfDayYesterday = yesterday.get(ChronoField.HOUR_OF_DAY)

  val schema = StructType(Seq(
    StructField("cookie_id", StringType),
    StructField("adv_id", IntegerType),
    StructField("adv_site", StringType),
    StructField("adv_price", DoubleType),
    StructField("os_system", StringType),
    StructField("os_family", StringType),
    StructField("ts", StringType)
  ))

  def cookieId = UUID.randomUUID().toString

  def price(p: Double): Double = {
    p + rnd.nextInt(100).toDouble / 10000
  }

  val input = new Random().shuffle(
    Seq.fill(5000)(Row(cookieId, 1, "cnn.com", price(0.1), "Yosemite", "OSX", now.toInstant.toEpochMilli.toString)) ++
    Seq.fill(4000)(Row(cookieId, 2, "bbc.com", price(0.3), "Mavericks", "OSX", now.toInstant.toEpochMilli.toString)) ++
    Seq.fill(400)(Row(cookieId, 3, "hbo.com", price(0.5), "Mavericks", "OSX", yesterday.toInstant.toEpochMilli.toString)) ++
    Seq.fill(200)(Row(cookieId, 4, "mashable.com", price(0.05), "MountainLion", "OSX", yesterday.toInstant.toEpochMilli.toString)) ++
    // first 4 sites contribute 96% of the rows into full data set
    Seq.fill(100)(Row(cookieId, 5, "gizmodo.com", price(0.14), "MountainLion", "OSX", yesterday.toInstant.toEpochMilli.toString)) ++
    Seq.fill(100)(Row(cookieId, 6, "reddit.com", price(0.21), "MountainLion", "OSX", yesterday.toInstant.toEpochMilli.toString)) ++
    Seq.fill(100)(Row(cookieId, 7, "amc.com", price(0.31), "MountainLion", "OSX", yesterday.toInstant.toEpochMilli.toString)) ++
    Seq.fill(100)(Row(cookieId, 8, "msnbc.com", price(0.1), "MountainLion", "OSX", yesterday.toInstant.toEpochMilli.toString)) ++
    // null columns should be skipped
    Seq.fill(100)(Row(cookieId, 9, null, null, "MountainLion", "OSX", null))
  )

  val isActive = true

  // Plain columns
  val adId = ModelFeature(isActive, "advertisement", "ad_id", "adv_id", Index(2.0, allOther = false))
  val adSite = ModelFeature(isActive, "advertisement", "ad_site", "adv_site", Top(95.0, allOther = true))
  val adPrice = ModelFeature(isActive, "advertisement", "ad_price", "adv_price", Bins(5, 0, 0))

  // Features with expressions
  val adIdSitePair = ModelFeature(isActive, "advertisement", "ad_id_site_pair", "concat('_', cast(adv_id as string), adv_site)", Top(95.0, allOther = false))
  val os = ModelFeature(isActive, "os", "os", "concat('_', os_system, os_family)", Top(100.0, allOther = false))
  val dayOfWeek = ModelFeature(isActive, "time", "day_of_week", "day_of_week(strToEpochMilli(ts), 'UTC')", Top(100.0, allOther = false))
  val hourOfDay = ModelFeature(isActive, "time", "hour_of_day", "hour_of_day(strToEpochMilli(ts), 'UTC')", Top(100.0, allOther = false))
  val notNullSite = ModelFeature(isActive, "expr", "not_null_ad_site", "nvl_str(adv_site, 'http://no-site.com')", Top(95.0, allOther = true))
  val notNullPrice = ModelFeature(isActive, "expr", "not_null_ad_price", "nvl(adv_price, 123.0)", Bins(5, 0, 0))
  val logPrice = ModelFeature(isActive, "expr", "log_price", "log(adv_price)", Bins(5, 0, 0))
  val greatestPrice = ModelFeature(isActive, "expr", "log_price", "greatest(adv_price, 0.1)", Bins(5, 0, 0))

  val df = Transformer.extractFeatures(
    session.createDataFrame(session.sparkContext.parallelize(input), schema),
    Seq(adId, adSite, adPrice, adIdSitePair, os, dayOfWeek, hourOfDay, notNullSite, notNullPrice, logPrice, greatestPrice)
  ) match {
    case -\/(err) => sys.error(s"Can't extract features: $err")
    case \/-(suc) => suc
  }

  object Transformers {
    val top = new TopTransformer(df)
    val index = new IndexTransformer(df)
    val bins = new BinsTransformer(df)

    def validate(f: ModelFeature*) = {
      f map (top.validate orElse index.validate orElse bins.validate)
    }
  }

  "Transformers" should "support all features" in {
    val valid = Transformers.validate(adId, adSite, adPrice, adIdSitePair, os, dayOfWeek, hourOfDay, notNullSite, notNullPrice, logPrice, greatestPrice)

    assert(valid.count(_.isRight) == 11)
    assert(valid(0) == TypedModelFeature(adId, IntegerType).right)
    assert(valid(1) == TypedModelFeature(adSite, StringType).right)
    assert(valid(2) == TypedModelFeature(adPrice, DoubleType).right)
    assert(valid(3) == TypedModelFeature(adIdSitePair, StringType).right)
    assert(valid(4) == TypedModelFeature(os, StringType).right)
    assert(valid(5) == TypedModelFeature(dayOfWeek, IntegerType).right)
    assert(valid(6) == TypedModelFeature(hourOfDay, IntegerType).right)
    assert(valid(7) == TypedModelFeature(notNullSite, StringType).right)
    assert(valid(8) == TypedModelFeature(notNullPrice, DoubleType).right)
    assert(valid(9) == TypedModelFeature(logPrice, DoubleType).right)
    assert(valid(10) == TypedModelFeature(greatestPrice, DoubleType).right)
  }

  it should "correctly extract nvl columns" in {
    val dataFrame = scalaz.Tag.unwrap(df)

    val sites = dataFrame.select(notNullSite.feature).collect().map(_.getString(0))
    assert(sites.count(_ == "http://no-site.com") == 100)

    val prices = dataFrame.select(notNullPrice.feature).collect().map(_.getDouble(0))
    assert(prices.count(_ > 100) == 100)
  }

  it should "calculate top columns with 'concat'" in {
    val typed = Transformers.top.validate(os).toOption.get
    val columns = Transformers.top.transform(typed)

    assert(columns.size == 3)

    assert(columns(0).columnId == 1)
    assert(columns(0).isInstanceOf[CategoricalValue])
    assert(columns(0).asInstanceOf[CategoricalValue].sourceName == "Yosemite_OSX")
    assert(columns(0).asInstanceOf[CategoricalValue].sourceValue == ModelMatrixEncoding.encode("Yosemite_OSX"))
    assert(columns(0).asInstanceOf[CategoricalValue].count == 5000)
    assert(columns(0).asInstanceOf[CategoricalValue].cumulativeCount == 5000)

    assert(columns(1).columnId == 2)
    assert(columns(1).isInstanceOf[CategoricalValue])
    assert(columns(1).asInstanceOf[CategoricalValue].sourceName == "Mavericks_OSX")
    assert(columns(1).asInstanceOf[CategoricalValue].sourceValue == ModelMatrixEncoding.encode("Mavericks_OSX"))
    assert(columns(1).asInstanceOf[CategoricalValue].count == 4400)
    assert(columns(1).asInstanceOf[CategoricalValue].cumulativeCount == 9400)

    assert(columns(2).columnId == 3)
    assert(columns(2).isInstanceOf[CategoricalValue])
    assert(columns(2).asInstanceOf[CategoricalValue].sourceName == "MountainLion_OSX")
    assert(columns(2).asInstanceOf[CategoricalValue].sourceValue == ModelMatrixEncoding.encode("MountainLion_OSX"))
    assert(columns(2).asInstanceOf[CategoricalValue].count == 700)
    assert(columns(2).asInstanceOf[CategoricalValue].cumulativeCount == 10100)
  }

  it should "calculate top day of weeks" in {
    val typed = Transformers.top.validate(dayOfWeek).toOption.get
    val columns = Transformers.top.transform(typed)

    assert(columns.size == 2)

    assert(columns(0).columnId == 1)
    assert(columns(0).isInstanceOf[CategoricalValue])
    assert(columns(0).asInstanceOf[CategoricalValue].sourceName.toInt == DayOfWeekNow)
    assert(columns(0).asInstanceOf[CategoricalValue].sourceValue == ModelMatrixEncoding.encode(DayOfWeekNow))

    assert(columns(1).columnId == 2)
    assert(columns(1).isInstanceOf[CategoricalValue])
    assert(columns(1).asInstanceOf[CategoricalValue].sourceName.toInt == DayOfWeekYesterday)
    assert(columns(1).asInstanceOf[CategoricalValue].sourceValue == ModelMatrixEncoding.encode(DayOfWeekYesterday))
  }

  it should "calculate top hour of days" in {
    val typed = Transformers.top.validate(hourOfDay).toOption.get
    val columns = Transformers.top.transform(typed)

    assert(columns.size == 2)

    assert(columns(0).columnId == 1)
    assert(columns(0).isInstanceOf[CategoricalValue])
    assert(columns(0).asInstanceOf[CategoricalValue].sourceName == HourOfDayNow.toString)

    assert(columns(1).columnId == 2)
    assert(columns(1).isInstanceOf[CategoricalValue])
    assert(columns(1).asInstanceOf[CategoricalValue].sourceName == HourOfDayYesterday.toString)

  }

}
