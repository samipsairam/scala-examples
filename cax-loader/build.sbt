name := "SparkSamples"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "3.1.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2"

retrieveManaged := true
