name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2" exclude("io.netty", "netty-all")
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.5.0-RC1"

libraryDependencies += "joda-time" % "joda-time" % "2.9.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

retrieveManaged := true

