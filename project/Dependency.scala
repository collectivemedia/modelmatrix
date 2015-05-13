import sbt._


object Dependency {

  // Versions
  object V {

    val Scopt              = "3.3.0"
    val Config             = "1.2.1"

    // Scalaz
    val Scalaz             = "7.1.1"

    // Spark
    val Spark              = "1.2.0-cdh5.3.3"

    // Test libraries
    val ScalaMock          = "3.2.1"
    val ScalaTest          = "2.2.4"
    val ScalaCheck         = "1.12.2"
  }


  // Compile Dependencies

  val scopt               = "com.github.scopt"          %% "scopt"                       % V.Scopt
  val config              = "com.typesafe"               % "config"                      % V.Config

  // Scalaz

  val scalazCore          = "org.scalaz"                %% "scalaz-core"                 % V.Scalaz

  // Spark

  val sparkYarn           = "org.apache.spark"          %% "spark-yarn"                  % V.Spark % "provided"
  val sparkHive           = "org.apache.spark"          %% "spark-hive"                  % V.Spark % "provided"

  // Test Dependencies

  object Test {

    val scalaMock         = "org.scalamock"             %% "scalamock-scalatest-support" % V.ScalaMock  % "it,test"
    val scalaTest         = "org.scalatest"             %% "scalatest"                   % V.ScalaTest  % "it,test"
    val scalaCheck        = "org.scalacheck"            %% "scalacheck"                  % V.ScalaCheck % "it,test"

  }

}
