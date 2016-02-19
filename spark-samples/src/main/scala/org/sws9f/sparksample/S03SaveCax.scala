package org.sws9f.sparksample

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._

object S03SaveCax extends DemoApp {
  
  case class WordCount(uid: String, date: String)
  
  val mysc = DemoApp().sc
  
  // mysc.setLogLevel("DEBUG")
  val distFile = mysc.textFile(textFilePath)
  
  // read test
  val myrdd = mysc.cassandraTable("ks1", "src_trans")
  println("READ %s rows".format(myrdd.count()))
  myrdd.unpersist()
  
  val collection = mysc.parallelize(Seq(WordCount("dog", "50"), WordCount("cow", "60")))
  println("Saving...")
  
  collection.saveAsCassandraTable("ks1", "t3", SomeColumns("uid", "date"))
  mysc.stop()
  
}
