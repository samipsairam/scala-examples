package org.sws9f.sparkcalc

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._

trait BaseApp extends App with Logging {

  val CassandraHost = "127.0.0.1"

  // Tell Spark the address of one Cassandra node:
  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", CassandraHost)
    .set("spark.cleaner.ttl", "3600")
    .set("spark.app.id", "SparkCalculator")
    .set("spark.executor.memory", "2g")
    .set("spark.cores.max", "4")
    .setMaster("local[5]")
    .setAppName(getClass.getSimpleName)

  // Connect to the Spark cluster:
  lazy val sc = new SparkContext(conf)
}

object BaseApp {
  def apply(): BaseApp = new BaseApp {}
}