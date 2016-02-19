package org.sws9f.sparksample

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object S04LoadCsv extends DemoApp {
  
  val myCsvPath = "/Users/larrysu/repos/data/trans_1m.csv"
  
  val sqlContext = new SQLContext(sc)
  
  val customSchema = StructType(Seq(
      StructField("uid", StringType, true),
      StructField("d1", DateType, true),
      StructField("c1", StringType, true),
      StructField("c2", StringType, true),
      StructField("c3", StringType, true),
      StructField("store_id", StringType, true),
      StructField("price", DoubleType, true),
      StructField("d_lead", DateType, true),
      StructField("d_next", DateType, true),
      StructField("diff", IntegerType, true)))

  val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true")
      .schema(customSchema)
      .load(myCsvPath)
  
  println("distFile count = %s".format(df.count()))
  
  println("rows : ")
  df.take(10).foreach(println)
  // read test
  // val rows : RDD[Row] = df.rdd
  
  println("create table : ")
  
  df.createCassandraTable(
      "ks1", 
      "t5", 
      partitionKeyColumns = Some(Seq("d1")), 
      clusteringKeyColumns = Some(Seq("c1","c2","c3","store_id")))
  
  println("saving rows...")
  
  df.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "t5", "keyspace" -> "ks1"))
    .save()
  
  println("saving rows done.")
    
  sc.stop()
  
}
