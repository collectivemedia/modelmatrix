disablePlugins(sbtassembly.AssemblyPlugin)

libraryDependencies ++= Dependencies.modelmatrixCore

// Configure MMC Schema migrations

flywayUrl := "jdbc:postgresql://localhost/modelmatrix"

flywayUser := "modelmatrix"

flywayPassword := "modelmatrix"

flywayTable := "mmc_schema_version"

flywaySqlMigrationPrefix := "v"
