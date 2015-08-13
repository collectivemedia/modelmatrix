enablePlugins(JavaAppPackaging)
enablePlugins(UniversalPlugin)

libraryDependencies ++= Dependencies.modelmatrixCli

// Assembly settings

mainClass := Some("com.collective.modelmatrix.cli.ModelMatrixCli")

assemblyJarName := "model-matrix-cli.jar"

// SBT Native Packager settings

name in Universal := "Model Matrix CLI"

packageName in Universal := "modelmatrix-cli"

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
