import sbt._

object VersionScheme {

  object Keys {

    val versionPrefix = Def.settingKey[String](
      "Prefix of the version string")

  }

}