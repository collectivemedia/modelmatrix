
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
    )

  val modelmatrixCli =
    Seq(
      scopt
    )

}
