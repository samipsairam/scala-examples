/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setMaster("local")
        .setAppName("AppLoadCassandra")
        .set("spark.cassandra.connection.host", "192.168.99.100")
        .setJars(List("target/scala-2.11/simple-project_2.11-1.0.jar"))
        .setSparkHome("/opt/sdata/spark/current")
    val sc = new SparkContext(conf)
    val myrdd = sc.cassandraTable("keyspace_name", "table_name")
    println("********READ!!!!!!!")
    println("********Count rows: %s".format(myrdd.count()))

    sc.stop()
  }
}

