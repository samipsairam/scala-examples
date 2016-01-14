/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/Users/larrysu/repos/scala-examples/spark-simple-app/mylogfile.md" // Should be some file on your system
    val sc = new SparkContext("local", "Simple App", "/opt/sdata/spark/current",
      List("target/scala-2.11/simple-project_2.11-1.0.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    sc.stop()
  }
}

