name := "SparkCalculator"

version := "1.0"

scalaVersion := "2.11.7"

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" exclude ("log4j", "log4j") exclude ("org.slf4j", "slf4j-log4j12")

// Cassandra
libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "3.0.2"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0-rc1"
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "1.5.0-RC1"

// utility : CSV
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2"
// utility : Logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

// utility : Config
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
// utility : IO
libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3"


dependencyOverrides += "org.apache.spark" %% "spark-core" % "1.6.0"
dependencyOverrides += "org.apache.spark" %% "spark-launcher" % "1.6.0"
dependencyOverrides += "org.apache.spark" %% "spark-network-common" % "1.6.0"
dependencyOverrides += "org.apache.spark" %% "spark-repl" % "1.6.0"
dependencyOverrides += "org.apache.spark" %% "spark-streaming" % "1.6.0"
dependencyOverrides += "org.apache.spark" %% "spark-catalyst" % "1.6.0"
dependencyOverrides += "org.apache.spark" %% "spark-network-shuffle" % "1.6.0"
dependencyOverrides += "org.apache.spark" %% "spark-sql" % "1.6.0"
dependencyOverrides += "org.apache.spark" %% "spark-unsafe" % "1.6.0"

// dependencyOverrides += "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0"

retrieveManaged := true
