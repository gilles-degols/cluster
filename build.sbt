name := "cluster"
organization := "net.degols.libs"
version := "1.1.1"

scalacOptions ++= Seq("-deprecation", "-feature", "-language:postfixOps")

scalaVersion := "2.12.8"
lazy val playVersion = "2.6.13"
lazy val electionLibraryVersion = "1.1.0"
val useLocalElectionLibrary = true

libraryDependencies += "com.typesafe.play" %% "play-json" % playVersion
libraryDependencies += "com.github.cb372" %% "scalacache-core" % "0.27.0"
libraryDependencies += "com.github.cb372" %% "scalacache-caffeine" % "0.27.0"
libraryDependencies += "commons-io" % "commons-io" % "2.4"

// Election library
val electionPath = "../election"
lazy val electionLibrary: RootProject = RootProject(file(electionPath))
val localElectionAvailable = scala.reflect.io.File(scala.reflect.io.Path(electionPath)).exists
lazy val cluster = if(localElectionAvailable && useLocalElectionLibrary) {
  (project in file(".")).dependsOn(electionLibrary)
} else {
  (project in file("."))
}

lazy val electionDependency = if(localElectionAvailable && useLocalElectionLibrary) {
  Seq()
} else {
  Seq("net.degols.libs" %% "election" % electionLibraryVersion exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12"))
}
libraryDependencies ++= electionDependency


// POM settings for Sonatype
sonatypeProfileName := "net.degols"
import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("gilles-degols", "cluster", "gilles@degols.net"))
licenses += ("MIT License", url("https://opensource.org/licenses/MIT"))
publishMavenStyle := true
publishTo := sonatypePublishToBundle.value
usePgpKeyHex("C0FAC2FE")

lazy val username = Option(System.getenv("SONATYPE_USER")).getOrElse("sonatype_user")
lazy val password = Option(System.getenv("SONATYPE_PASSWORD")).getOrElse("sonatype_password")
credentials += Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)
