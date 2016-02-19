/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.sql.Timestamp

object ImportCsvApp {
  case class Transax(uid: String, date:Timestamp, store_cate1:String, store_cate2:String, store_cate3:String, 
    store_id:String, payment:String, lead_date:String, next_date:String, diff:Option[Int])
    
  case class WordCount(uid: String, date: String)
  
  def main(args: Array[String]) {
    val conf = new SparkConf(true)
        .setMaster("local[4]")
        .setAppName("AppLoadCassandra")
        .set("spark.executor.memory", "4g")
        .set("spark.app.id", "SparkLoadCax")
        .set("spark.cleaner.ttl", "3600")
        .set("spark.cassandra.connection.host", "192.168.99.100")
        .setJars(List("target/scala-2.11/simple-project_2.11-1.0.jar"))
        .setSparkHome("/opt/sdata/spark/current")
    val sc = new SparkContext(conf)
    
    sc.setLogLevel("INFO")
    //val myrdd = sc.cassandraTable("ks1", "src_trans")
    
    var pathSrcCsv = "/Users/larrysu/repos/data/trans_1k.csv"
    
    //val collection = sc.parallelize(Seq(WordCount("dog", "50"), WordCount("cow", "60")))
    // rddSource.saveToCassandra("ks1", "t2", SomeColumns("uid", "date"))
    
        //
    // val formatter = DateTimeFormat.forPattern("yyyy-mm-dd HH:mm:ss 'UTC'")
    // // formatter.parseDateTime("2013-01-28 00:00:00 UTC")
    //
    //
    //
    // var pathSrcCsv = "/Users/larrysu/repos/data/trans_1k.csv"
    // val rddSource = sc.textFile(pathSrcCsv).map(_.split(",")).map(p => Transax(p(0),
    //   Timestamp.valueOf(p(1).replace(" UTC", "")),
    //   p(2), p(3), p(4),
    //   p(5), p(6), p(7), p(8), if (p(9)=="NULL") None else Some(p(9).toInt) ))
    //
    // println("pathSrcCsv")
    // println("********Source CSV Count rows: %s".format(rddSource.count()))
    //
    // println("********1st rows: %s".format(rddSource.first()))

    
    //
    // rddSource.saveToCassandra("ks1", "t1", SomeColumns("uid", "date") )
    //
    // val myrdd = sc.cassandraTable("ks1", "t1")
    // println("********Dist Cax Count rows: %s".format(myrdd.count()))
    //
    // rddSource.saveToCassandra("ks1", "src_trans", SomeColumns("uid", "date", "store_cate1", "store_cate2", "store_cate3",
    //   "store_id", "payment", "lead_date", "next_date", "diff"))
    //
    // val myrdd = sc.cassandraTable("ks1", "src_trans")
    // println("********Dist Cax Count rows: %s".format(myrdd.count()))


    sc.stop()
  }
}

