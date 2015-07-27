
object Dependencies {

  import Dependency._

  val spark = Seq(
      sparkYarn
    , sparkHive
    , sparkMLLib
  )

  val test = Seq(
      Test.scalaTest
    , Test.scalaMock
    , Test.scalaCheck
    , h2
  )

  // Project dependencies

  val modelmatrixCore =
    spark ++
    test ++
    Seq(
      slf4jApi
    , scalazCore
    , scalazStream
    , config
    , scodecBits
    , slick
    , pgDriver
    , hikariCP
    , h2
    )

  val modelmatrixCli =
    spark ++
    test ++
    Seq(
      scopt
    , guava
    , scalazStream
    , flyway
    )

  val modelmatrixUdf =
    Seq(scodecBits)

}
