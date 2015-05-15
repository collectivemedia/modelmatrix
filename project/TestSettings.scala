import sbt.Keys._
import sbt._
import org.scalastyle.sbt.ScalastylePlugin


object TestSettings {

  private[this] lazy val checkScalastyle = taskKey[Unit]("checkScalastyle")

  // Custom IntegrationTest config that shares code with Test
  val IntegrationTest = config("it") extend Test

  def testSettings: Seq[Def.Setting[_]] = Seq(
    // Run Scalastyle as a part of tests
    checkScalastyle := ScalastylePlugin.scalastyle.in(Compile).toTask("").value,
    test in Test <<= (test in Test) dependsOn checkScalastyle,
    // Disable logging in all tests
    javaOptions in Test += "-Dlog4j.configuration=log4j-turned-off.properties",
    // Generate JUnit test reports
    testOptions in Test <+= (target in Test) map {
      t => Tests.Argument(TestFrameworks.ScalaTest, "-u", (t / "test-reports").toString)
    }
  )

  def integrationTestSettings: Seq[Def.Setting[_]] = Defaults.itSettings ++ Seq(
    // Run Scalastyle as a part of tests
    checkScalastyle := ScalastylePlugin.scalastyle.in(Compile).toTask("").value,
    test in IntegrationTest <<= (test in IntegrationTest) dependsOn checkScalastyle,
    // Fork in IT to run Spark Tests
    fork in IntegrationTest := true,
    // Disable logging in all tests
    javaOptions in IntegrationTest += "-Dlog4j.configuration=log4j-turned-off.properties",
    // Generate JUnit test reports
    testOptions in IntegrationTest <+= (target in IntegrationTest) map {
      t => Tests.Argument(TestFrameworks.ScalaTest, "-u", (t / "test-reports").toString)
    }
  )
}
