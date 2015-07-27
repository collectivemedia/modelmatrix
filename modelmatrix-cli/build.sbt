enablePlugins(JavaAppPackaging)
enablePlugins(UniversalPlugin)

libraryDependencies ++= Dependencies.modelmatrixCli

// Assembly settings

mainClass := Some("com.collective.modelmatrix.cli.ModelMatrixCli")

assemblyJarName := "model-matrix-cli.jar"

// SBT Native Packager settings

name in Universal := "Model Matrix CLI"

// removes all jar mappings in universal and appends the fat jar
mappings in Universal := {
  // universalMappings: Seq[(File,String)]
  val universalMappings = (mappings in Universal).value
  val fatJar = (assembly in Compile).value
  // removing means filtering
  val filtered = universalMappings filter {
    case (file, fileName) =>  ! fileName.endsWith(".jar") && ! fileName.endsWith(".bat")
  }
  // add the fat jar
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}


// the bash scripts classpath only needs the fat jar
scriptClasspath := Seq( (assemblyJarName in assembly).value )

// Configure MMC Schema migrations
//
//flywayUrl := "jdbc:postgresql://localhost/modelmatrix"
//
//flywayUser := "modelmatrix"
//
//flywayPassword := "modelmatrix"
//
//flywayTable := "mmc_schema_version"
//
//flywaySqlMigrationPrefix := "v"
//
//// Select the location dynamically base on the url
//// This is just used during itegration testing so in the end we could default it to PG
//flywayLocations := Seq("db/migration/" + {
// flywayUrl.value match {
//   case pg if pg.startsWith("jdbc:postgresql:") => "pg"
//   case h2 if h2.startsWith("jdbc:h2:") => "h2"
//   case _ => sys.error(s"Unsupported database migration url: ${flywayUrl.value}")
// }
//})
