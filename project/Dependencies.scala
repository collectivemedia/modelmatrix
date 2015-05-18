
object Dependencies {

  import Dependency._

  val spark = Seq(
      sparkYarn
    , sparkHive
  )

  val test = Seq(
      Test.scalaTest
    , Test.scalaMock
    , Test.scalaCheck
    , Test.h2
  )

  // Project dependencies

  val modelmatrixCore =
    spark ++
    test ++
    Seq(
      slf4jApi
    , scalazCore
    , config
    , ficus
    , slick
    , pgDriver
    , hikariCP
    )

  val modelmatrixCli =
    test ++
    Seq(
      log4j
    , slf4jLog4j
    , scopt
    , guava
    )

}
