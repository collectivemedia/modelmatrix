import VersionScheme.Keys._

isRelease in ThisBuild := sys.props("release") == "true"

versionPrefix in ThisBuild := "0.1.0"

version in ThisBuild <<= Def.setting[String] {
  if (isRelease.value) {
    versionPrefix.value
  } else  {
    val headSha = GitHelper.headSha()
    s"${versionPrefix.value}.$headSha"
  }
}
