name := "SparkSamples"

version := "1.0"

scalaVersion := "2.11.7"

// libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
// libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.5.0-RC1"
// libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.3.0"
libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "3.1.1"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0"

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2"

retrieveManaged := true
