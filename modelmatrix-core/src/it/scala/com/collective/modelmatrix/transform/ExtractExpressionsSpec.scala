package com.collective.modelmatrix.transform

import java.time.temporal.{ChronoField, ChronoUnit}
import java.time.{DayOfWeek, Instant, ZoneOffset}
import java.util.UUID

import com.collective.modelmatrix.CategorialColumn.CategorialValue
import com.collective.modelmatrix.{ModelFeature, ModelMatrix, TestSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec
import scodec.bits.ByteVector

import scala.util.Random
import scalaz.syntax.either._

class ExtractExpressionsSpec extends FlatSpec with TestSparkContext {

  val sqlContext = ModelMatrix.sqlContext(sc)

  val rnd = new Random()

  val now = Instant.now().atZone(ZoneOffset.UTC)
  val yesterday = now.minus(1, ChronoUnit.DAYS).minus(1, ChronoUnit.HOURS).withZoneSameInstant(ZoneOffset.UTC)

  val DayOfWeekNow = DayOfWeek.from(now).toString
  val DayOfWeekYesterday = DayOfWeek.from(yesterday).toString

  val HourOfDayNow = now.get(ChronoField.HOUR_OF_DAY)
  val HourOfDayYesterday = yesterday.get(ChronoField.HOUR_OF_DAY)

  val schema = StructType(Seq(
    StructField("cookie_id", StringType),
    StructField("adv_id", IntegerType),
    StructField("adv_site", StringType),
    StructField("adv_price", DoubleType),
    StructField("os_system", StringType),
    StructField("os_family", StringType),
    StructField("ts", LongType)
  ))

  def cookieId = UUID.randomUUID().toString

  def price(p: Double): Double = {
    p + rnd.nextInt(100).toDouble / 10000
  }

  val input = new Random().shuffle(
    Seq.fill(5000)(Row(cookieId, 1, "cnn.com", price(0.1), "Yosemite", "OSX", now.toInstant.toEpochMilli)) ++
    Seq.fill(4000)(Row(cookieId, 2, "bbc.com", price(0.3), "Mavericks", "OSX", now.toInstant.toEpochMilli)) ++
    Seq.fill(400)(Row(cookieId, 3, "hbo.com", price(0.5), "Mavericks", "OSX", yesterday.toInstant.toEpochMilli)) ++
    Seq.fill(200)(Row(cookieId, 4, "mashable.com", price(0.05), "MountainLion", "OSX", yesterday.toInstant.toEpochMilli)) ++
    // first 4 sites contribute 96% of the rows into full data set
    Seq.fill(100)(Row(cookieId, 5, "gizmodo.com", price(0.14), "MountainLion", "OSX", yesterday.toInstant.toEpochMilli)) ++
    Seq.fill(100)(Row(cookieId, 6, "reddit.com", price(0.21), "MountainLion", "OSX", yesterday.toInstant.toEpochMilli)) ++
    Seq.fill(100)(Row(cookieId, 7, "amc.com", price(0.31), "MountainLion", "OSX", yesterday.toInstant.toEpochMilli)) ++
    Seq.fill(100)(Row(cookieId, 8, "msnbc.com", price(0.1), "MountainLion", "OSX", yesterday.toInstant.toEpochMilli)) ++
    // null columns should be skipped
    Seq.fill(100)(Row(cookieId, 9, null, price(0.1), "MountainLion", "OSX", null))
  )

  val isActive = true

  val adId = ModelFeature(isActive, "advertisement", "ad_id", "adv_id", Index(2.0, allOther = false))
  val adSite = ModelFeature(isActive, "advertisement", "ad_site", "adv_site", Top(95.0, allOther = true))
  val adPrice = ModelFeature(isActive, "advertisement", "ad_price", "adv_price", Bins(5, 0, 0))
  val os = ModelFeature(isActive, "os", "os", "concat('_', os_system, os_family)", Top(100.0, allOther = false))
  val dayOfWeek = ModelFeature(isActive, "time", "day_of_week", "day_of_week(ts, 'UTC')", Top(100.0, allOther = false))
  val hourOfDay = ModelFeature(isActive, "time", "hour_of_day", "hour_of_day(ts, 'UTC')", Top(100.0, allOther = false))

  val df = Transformer.selectFeatures(
    sqlContext.createDataFrame(sc.parallelize(input), schema),
    Seq(adId, adSite, adPrice, os, dayOfWeek, hourOfDay)
  )

  object Transformers {
    val top = new TopTransformer(df)
    val index = new IndexTransformer(df)
    val bins = new BinsTransformer(df)

    def validate(f: ModelFeature*) = {
      f map (top.validate orElse index.validate orElse bins.validate)
    }
  }

  "Transformers" should "support all features" in {
    val valid = Transformers.validate(adId, adSite, adPrice, os, dayOfWeek, hourOfDay)
    assert(valid.count(_.isRight) == 6)
    assert(valid(0) == TypedModelFeature(adId, IntegerType).right)
    assert(valid(1) == TypedModelFeature(adSite, StringType).right)
    assert(valid(2) == TypedModelFeature(adPrice, DoubleType).right)
    assert(valid(3) == TypedModelFeature(os, StringType).right)
    assert(valid(4) == TypedModelFeature(dayOfWeek, StringType).right)
    assert(valid(5) == TypedModelFeature(hourOfDay, IntegerType).right)
  }

  it should "calculate top columns with 'concat'" in {
    val typed = Transformers.top.validate(os).toOption.get
    val columns = Transformers.top.transform(typed)

    assert(columns.size == 3)

    assert(columns(0).columnId == 1)
    assert(columns(0).isInstanceOf[CategorialValue])
    assert(columns(0).asInstanceOf[CategorialValue].sourceName == "Yosemite_OSX")
    assert(columns(0).asInstanceOf[CategorialValue].sourceValue == ByteVector("Yosemite_OSX".getBytes))
    assert(columns(0).asInstanceOf[CategorialValue].count == 5000)
    assert(columns(0).asInstanceOf[CategorialValue].cumulativeCount == 5000)

    assert(columns(1).columnId == 2)
    assert(columns(1).isInstanceOf[CategorialValue])
    assert(columns(1).asInstanceOf[CategorialValue].sourceName == "Mavericks_OSX")
    assert(columns(1).asInstanceOf[CategorialValue].sourceValue == ByteVector("Mavericks_OSX".getBytes))
    assert(columns(1).asInstanceOf[CategorialValue].count == 4400)
    assert(columns(1).asInstanceOf[CategorialValue].cumulativeCount == 9400)

    assert(columns(2).columnId == 3)
    assert(columns(2).isInstanceOf[CategorialValue])
    assert(columns(2).asInstanceOf[CategorialValue].sourceName == "MountainLion_OSX")
    assert(columns(2).asInstanceOf[CategorialValue].sourceValue == ByteVector("MountainLion_OSX".getBytes))
    assert(columns(2).asInstanceOf[CategorialValue].count == 700)
    assert(columns(2).asInstanceOf[CategorialValue].cumulativeCount == 10100)
  }

  it should "calculate top day of weeks" in {
    val typed = Transformers.top.validate(dayOfWeek).toOption.get
    val columns = Transformers.top.transform(typed)

    assert(columns.size == 2)

    assert(columns(0).columnId == 1)
    assert(columns(0).isInstanceOf[CategorialValue])
    assert(columns(0).asInstanceOf[CategorialValue].sourceName == DayOfWeekNow)
    assert(columns(0).asInstanceOf[CategorialValue].sourceValue == ByteVector(DayOfWeekNow.getBytes))

    assert(columns(1).columnId == 2)
    assert(columns(1).isInstanceOf[CategorialValue])
    assert(columns(1).asInstanceOf[CategorialValue].sourceName == DayOfWeekYesterday)
    assert(columns(1).asInstanceOf[CategorialValue].sourceValue == ByteVector(DayOfWeekYesterday.getBytes))
  }

  it should "calculate top hour of days" in {
    val typed = Transformers.top.validate(hourOfDay).toOption.get
    val columns = Transformers.top.transform(typed)

    assert(columns.size == 2)

    assert(columns(0).columnId == 1)
    assert(columns(0).isInstanceOf[CategorialValue])
    assert(columns(0).asInstanceOf[CategorialValue].sourceName == HourOfDayNow.toString)

    assert(columns(1).columnId == 2)
    assert(columns(1).isInstanceOf[CategorialValue])
    assert(columns(1).asInstanceOf[CategorialValue].sourceName == HourOfDayYesterday.toString)

  }

}
