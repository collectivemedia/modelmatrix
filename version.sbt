import VersionScheme.Keys._

versionPrefix in ThisBuild := "0.0.1"

version in ThisBuild <<= Def.setting[String] {
  val headSha = GitHelper.headSha()
  s"${versionPrefix.value}.${headSha}"
}
