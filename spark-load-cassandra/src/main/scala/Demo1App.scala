/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.sql.Timestamp

object Demo1App {
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
  
    val words = "/Users/larrysu/repos/data/trans_1k.csv"
    
    CassandraConnector(conf).withSessionDo { session =>
        session.execute(s"CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute(s"CREATE TABLE IF NOT EXISTS demo.wordcount (word TEXT PRIMARY KEY, count COUNTER)")
        session.execute(s"TRUNCATE demo.wordcount")
    }

    val myrdd = sc.textFile(words)
      .flatMap(line => line.split(","))
      .map(word => (word, 1))
      //.reduceByKey( (a,b) => a + b )
      //.saveToCassandra("demo", "wordcount")
      

    println("********MyRDD: %s".format(myrdd.first()))

    // print out the data saved from Spark to Cassandra
    //sc.cassandraTable("demo", "wordcount").collect.foreach(println)
    sc.stop()

  }
}

