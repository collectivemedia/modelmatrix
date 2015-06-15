name in ThisBuild := "Model Matrix"

organization in ThisBuild := "com.collective.modelmatrix"

scalaVersion in ThisBuild := "2.10.5"

scalacOptions in ThisBuild ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked"
  //"-Ywarn-unused-import" // turn on whenever you want to discover unused imports, 2.11+
  //"-Xfuture"
  //"-Xlint",
  //"-Ywarn-value-discard"
)

// compiler optimization (2.11+ only)
// still experimental
// scalacOptions       += "-Ybackend:o3"

licenses in ThisBuild += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

scalastyleFailOnError in ThisBuild := true

maxErrors in ThisBuild := 5

shellPrompt in ThisBuild := ShellPrompt.buildShellPrompt

resolvers in ThisBuild ++= Seq(
  // Typesafe
  Resolver.typesafeRepo("releases"),
  Resolver.typesafeRepo("snapshots"),
  Resolver.typesafeIvyRepo("releases"),
  Resolver.typesafeIvyRepo("snapshots"),
  // Sonatype
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  // Scalaz
  "Scalaz Bintray Repo"  at "http://dl.bintray.com/scalaz/releases",
  // Apache
  "Apache Releases"      at "http://repository.apache.org/content/repositories/releases/",
  "Apache Snapshots"     at "http://repository.apache.org/content/repositories/snapshots",
  // Cloudera
  "Cloudera"             at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  // Conjars
  "Conjars"              at "http://conjars.org/repo"
)


// Model Matrix project

def ModelMatrixProject(path: String) =
  Project(path, file(path))
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .configs(TestSettings.IntegrationTest)
    .settings(TestSettings.testSettings: _*)
    .settings(TestSettings.integrationTestSettings: _*)
    .settings(bintrayRepository := "releases")
    .settings(bintrayOrganization := Some("collectivemedia"))


// Aggregate all projects & disable publishing root project

lazy val root = Project("modelmatrix", file(".")).
  settings(publish :=()).
  settings(publishLocal :=()).
  settings(unidocSettings: _*).
  aggregate(modelmatrixCore, modelmatrixCli, modelMatrixClient)


// Model Matrix projects

lazy val modelmatrixCore =
  ModelMatrixProject("modelmatrix-core")

lazy val modelmatrixCli =
  ModelMatrixProject("modelmatrix-cli")
    .dependsOn(modelmatrixCore)

lazy val modelMatrixClient =
  ModelMatrixProject("modelmatrix-client")
    .dependsOn(modelmatrixCore)
