package org.sws9f.sparksample

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._

trait DemoApp extends App with Logging {

  val textFilePath = "/Users/larrysu/repos/data/trans_1k.csv"

  val CassandraHost = "192.168.99.100"

  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraHost)
    .set("spark.cleaner.ttl", "3600")
    .set("spark.app.id", "SparkSampleX")
    .setMaster("local[1]")
    .setAppName(getClass.getSimpleName)

  // Connect to the Spark cluster:
  lazy val sc = new SparkContext(conf)
}

object DemoApp {
  def apply(): DemoApp = new DemoApp {}
}