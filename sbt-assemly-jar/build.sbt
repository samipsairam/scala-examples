lazy val root = (project in file(".")).
  settings(
    name := "fat-jar",
    version := "1.0",
    scalaVersion := "2.11.7",
    organization := "org.sws9f"
)

// for logging library
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.10"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

