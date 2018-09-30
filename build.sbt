import play.sbt.PlayScala
import sbt.RootProject

name := "cluster"
organization := "net.degols.filesgate.libs"
version := "0.0.1"

scalacOptions ++= Seq("-deprecation", "-feature", "-language:postfixOps")

scalaVersion := "2.12.1"
lazy val playVersion = "2.6.1"
lazy val akkaVersion = "2.5.2"

libraryDependencies += "com.google.inject" % "guice" % "3.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12")
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.19"
)

libraryDependencies += "joda-time" % "joda-time" % "2.10"

libraryDependencies += "commons-io" % "commons-io" % "2.4"

// Election library
val electionLibraryVersion = "0.0.1"
val electionPath = "../election"
lazy val electionLibrary: RootProject = RootProject(file(electionPath))
val useLocalElectionLibrary = true
val localElectionAvailable = scala.reflect.io.File(scala.reflect.io.Path(electionPath)).exists
lazy val proj = if(localElectionAvailable && useLocalElectionLibrary) {
  (project in file(".")).enablePlugins(PlayScala).dependsOn(electionLibrary)
} else {
  (project in file(".")).enablePlugins(PlayScala)
}

lazy val electionDependency = if(localElectionAvailable && useLocalElectionLibrary) {
  Seq()
} else {
  Seq("net.degols" %% "election" % electionLibraryVersion exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12"))
}
libraryDependencies ++= electionDependency


// Akka Remoting
libraryDependencies += "com.typesafe.akka" %% "akka-remote" % akkaVersion
