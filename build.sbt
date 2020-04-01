import sbtrelease.ReleaseStateTransformations._

lazy val Vers = new {
  val scalatest = "3.0.1"
  val resources = "src/main/resources/old"
  val scalaADT  = "src/main/scala"
}

lazy val commonSettings = Seq(
  name := "avro-ddl-tool",
  scalaVersion := "2.12.10",
  organization := "io.github.pascals-ager",
  scalaVersion := "2.12.10",
  crossScalaVersions := Seq("2.11.11", "2.12.10"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-Ypartial-unification"),
  addCompilerPlugin(
    "org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full
  ),
  sourceGenerators in Compile += (avroScalaGenerate in Compile).taskValue,
  avroScalaSource in Compile := new java.io.File(
    s"${baseDirectory.value}/${Vers.scalaADT}"
  ),
  avroSourceDirectories in Compile += new java.io.File(
    s"${baseDirectory.value}/${Vers.resources}"
  )
)

lazy val root = (project in file("."))
  .settings(moduleName := "avro-ddl-tool")
  .settings(commonSettings: _*)
  .settings(pgpPassphrase := Some(System.getenv("GPG_PASSPHRASE").toCharArray))
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang"             % "scala-reflect"   % scalaVersion.value,
      "org.scala-lang"             % "scala-compiler"  % scalaVersion.value % Provided,
      "org.scalactic"              %% "scalactic"      % Vers.scalatest % Test,
      "org.scalatest"              %% "scalatest"      % Vers.scalatest % Test,
      "ch.qos.logback"             % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging"  % "3.9.2",
      "org.tpolecat"               %% "doobie-core"    % "0.7.0" exclude ("log4j", "log4j") exclude ("sl4j", "sl4j"),
      "org.apache.hive"            % "hive-jdbc"       % "2.3.3" exclude ("log4j", "log4j") exclude ("sl4j", "sl4j"),
      "org.apache.avro"            % "avro"            % "1.8.0" exclude ("log4j", "log4j") exclude ("sl4j", "sl4j"),
      "org.kitesdk"                % "kite-data-core"  % "1.1.0" exclude ("log4j", "log4j") exclude ("sl4j", "sl4j")
    )
  )

licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

homepage := Some(url("https://github.com/pascals-ager/avro-ddl-tool"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/pascals-ager/avro-ddl-tool"),
    "scm:git@github.com:pascals-ager/avro-ddl-tool.git"
  )
)

developers := List(
  Developer(
    id    = "io.github.pascals-ager",
    name  = "pascalsAger",
    email = "pascals.ager20@gmail.com",
    url   = url("https://github.com/pascals-ager")
  )
)

credentials += Credentials("Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  System.getenv("SONATYPE_USERNAME"),
  System.getenv("SONATYPE_PASSWORD"))

usePgpKeyHex(System.getenv("GPG_PUBLIC_KEY"))

scalafmtOnCompile := true
publishMavenStyle := true
publishArtifact in Test := false
publishTo := sonatypePublishToBundle.value
pomIncludeRepository := { _ =>
  false
}

releaseTagComment := s"* Test Releasing ${(version in ThisBuild).value} [skip ci]"

releaseCommitMessage := s"* Test Setting version to ${(version in ThisBuild).value} [skip ci]"

val runUnitTests = ReleaseStep(
  action = Command.process(
    "testOnly * -- -l \"io.github.pascals.avro.schema.tags.IntegrationTest\"",
    _
  ),
  enableCrossBuild = true
)

val runIntegrationTests = ReleaseStep(
  action = Command.process(
    "testOnly * -- -n \"io.github.pascals.avro.schema.tags.IntegrationTest\"",
    _
  ),
  enableCrossBuild = true
)

val publishJar =
  ReleaseStep(action = Command.process("publish", _), enableCrossBuild = true)

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runUnitTests,
  setReleaseVersion,
  publishJar
)
