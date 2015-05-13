
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
  )

  // Project dependencies

  val modelMatrix =
    spark ++
    test ++
    Seq(
      scopt
    , scalazCore
    , config
    )
}
