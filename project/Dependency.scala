import sbt._


object Dependency {

  // Versions
  object V {

    val Slf4j              = "1.7.12"
    val Config             = "1.3.0"
    val Scopt              = "3.3.0"
    val ScodecBits         = "1.0.6"
    val Flyway             = "3.2.1"
    val Guava              = "14.0.1" // match to Spark Guava version

    // Spark
    val Spark              = "1.3.0-cdh5.4.2"

    // Database
    val Slick              = "3.0.0"
    val PgDriver           = "9.4-1201-jdbc41"
    val HikariCP           = "2.3.5"

    // Scalaz
    val Scalaz             = "7.1.1"
    val ScalazStream       = "0.7a"

    // Test libraries
    val H2                 = "1.4.187"
    val ScalaMock          = "3.2.1"
    val ScalaTest          = "2.2.4"
    val ScalaCheck         = "1.12.2"
  }


  // Compile Dependencies

  val slf4jApi            = "org.slf4j"                  % "slf4j-api"                   % V.Slf4j
  val config              = "com.typesafe"               % "config"                      % V.Config
  val scopt               = "com.github.scopt"          %% "scopt"                       % V.Scopt
  val scodecBits          = "org.scodec"                %% "scodec-bits"                 % V.ScodecBits
  val flyway              = "org.flywaydb"               % "flyway-core"                 % V.Flyway
  val guava               = "com.google.guava"           % "guava"                       % V.Guava force()

  // Database
  val slick               = "com.typesafe.slick"        %% "slick"                       % V.Slick
  val pgDriver            = "org.postgresql"             % "postgresql"                  % V.PgDriver
  val hikariCP            = "com.zaxxer"                 % "HikariCP"                    % V.HikariCP

  // Scalaz

  val scalazCore          = "org.scalaz"                %% "scalaz-core"                 % V.Scalaz
  val scalazStream        = "org.scalaz.stream"         %% "scalaz-stream"               % V.ScalazStream

  // Spark

  val sparkYarn           = "org.apache.spark"          %% "spark-yarn"                  % V.Spark % "provided"
  val sparkHive           = "org.apache.spark"          %% "spark-hive"                  % V.Spark % "provided"
  val sparkMLLib          = "org.apache.spark"          %% "spark-mllib"                 % V.Spark % "provided"

  // Test Dependencies

  object Test {

    val scalaMock         = "org.scalamock"             %% "scalamock-scalatest-support" % V.ScalaMock  % "it,test"
    val scalaTest         = "org.scalatest"             %% "scalatest"                   % V.ScalaTest  % "it,test"
    val scalaCheck        = "org.scalacheck"            %% "scalacheck"                  % V.ScalaCheck % "it,test"
    val h2                = "com.h2database"             % "h2"                          % V.H2         % "it,test"

  }

}
