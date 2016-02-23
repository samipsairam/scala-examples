package org.sws9f.sparkcalc

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector._

// import com.datastax.spark.connector._
import scala.collection.mutable.ListBuffer
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/opt/sdata/spark/current/README.md" // Should be some file on your system
    val confSpark = new SparkConf().setAppName("Simple Application")
    
    val confApp = ConfigFactory.load()
    confApp.getObject("spark").foreach (
      {
        case (k, v) => { 
          println( k + ":--" + v.unwrapped )
          confSpark.set(k, v.unwrapped.toString)
        }
      }
    )
    
    val sc = new SparkContext(confSpark)
    val rdd1 = sc.cassandraTable[Tranx]("ks1", "tranx").where("date < ?", "2014-03-01")
    
    println("============================== loading RDD...")
    
    var countRows = rdd1.count
    println("============================== Count rows: " + countRows)
    sc.stop
    // val logData = sc.textFile(logFile, 2).cache()
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    // println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

object SparkBase extends App {

  val sparkConf = new SparkConf(true)
  val conf = ConfigFactory.load()
  conf.getObject("spark").foreach (
    {
      case (k, v) => { 
        println( k + ":--" + v.unwrapped )
        sparkConf.set(k, v.unwrapped.toString)
      }
    }
  )
  
  // val settingSchema = conf.getString("spark.").stripMargin

  // Tell Spark the address of one Cassandra node:
  // val conf = new SparkConf(true)
  //   .set("spark.cassandra.connection.host", CassandraHost)
  //   .set("spark.cleaner.ttl", "3600")
  //   .set("spark.app.id", "SparkCalculator")
  //   .set("spark.executor.memory", "2g")
  //   .set("spark.cores.max", "4")
  //   .setMaster("local[5]")
  //   .setAppName(getClass.getSimpleName)
    //
  // lazy val conf = loadConf(args)
  //
  // def loadConf( args: Array[String] ) = {
  //   println("config...")
  //   if (args == null) null
  //   if (args.length > 0) args.foreach(println) else null
  // }
  //
  // def main(args: Array[String]): Unit = {
  println("Hello, world!")
  
  // }

  // lazy val conf = {
  //   new SparkConf(true)
  //
  // }

  def parseArgs(args: Array[String]) : Array[(String, String)] = {
    val params = new ListBuffer[(String, String)]()
    // for ( arg <- args if arg.contains("=") )
    for ( arg <- args if arg.contains("=") ) {
      val elems = arg.split("=")
      val oneParam = (elems(0), elems(1))
      params += oneParam
    }
    return params.toArray 
  }


  // Connect to the Spark cluster:
  // lazy val sc = new SparkContext(conf)
  // lazy val sqlContext = new SQLContext(sc)
}
