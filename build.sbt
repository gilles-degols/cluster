name := "cluster"
organization := "net.degols.libs"
version := "1.0.0"

scalacOptions ++= Seq("-deprecation", "-feature", "-language:postfixOps")

scalaVersion := "2.12.1"
lazy val playVersion = "2.6.1"
lazy val akkaVersion = "2.5.2"

libraryDependencies += "com.google.inject" % "guice" % "3.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % playVersion

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
val electionLibraryVersion = "1.0.0"
val electionPath = "../election"
lazy val electionLibrary: RootProject = RootProject(file(electionPath))
val useLocalElectionLibrary = true
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

// Mongo Java Driver
// https://mvnrepository.com/artifact/org.mongodb/mongo-java-driver
libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.7.0"

// Akka Remoting
libraryDependencies += "com.typesafe.akka" %% "akka-remote" % akkaVersion

// Allow temporary overwrite
// publishConfiguration := publishConfiguration.value.withOverwrite(true)
// publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
publishTo := Some("gd-maven" at s"http://localhost:8081/repository/maven-gd")
isSnapshot := false
credentials += Credentials("Sonatype Nexus Repository Manager", "localhost", "admin", "admin123")
resolvers += "gd-maven" at "http://localhost:8081/repository/maven-gd/"
