resolvers += "Flyway" at "http://flywaydb.org/repo"

libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.5"

// Dependency graph
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.5")

// Assembly Fat jar for Spark
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")

// Check Scala style
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.7.0")

// Publish unified documentation to site
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.2")

// Publish to bintray
addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")

// Package CLI application
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.2")
