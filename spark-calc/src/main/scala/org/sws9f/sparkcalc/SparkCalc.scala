package org.sws9f.sparkcalc

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
import java.time.LocalDate
import java.sql.Date

object SparkCalc extends BaseApp {
  
  val mysc = BaseApp().sc
  
  // mysc.setLogLevel("DEBUG")
  
  // read test
  val myrdd = mysc.cassandraTable("ks1", "trans")
  //println("READ %s rows".format(myrdd.count()))
  println(myrdd.first())
  println("shutting-down.............")
  myrdd.unpersist()
  mysc.stop()
  
}

// class Tranx (
//   var uid: String,
//   date: com.datastax.driver.core.LocalDate,
//   var c1: String,
//   var c2: String,
//   var c3: String,
//   var sid: String,
//   var payment: Double,
//   ldate: Option[com.datastax.driver.core.LocalDate],
//   ndate: Option[com.datastax.driver.core.LocalDate],
//   var diff: Option[Int]) extends Serializable {
//
//   // var uid: String = cuid
//   var vdate: LocalDate = LocalDate.ofEpochDay(date.getDaysSinceEpoch.toLong)
//   // var c1: String = cc1
//   // var c2: String = cc2
//   // var c3: String = cc3
//   // var sid: String = csid
//   // var payment: Double = cpayment
//   var vldate: Option[LocalDate] = if (ldate == None) None else Some(LocalDate.ofEpochDay(ldate.get.getDaysSinceEpoch.toLong))
//   var vndate: Option[LocalDate] = if (ndate == None) None else Some(LocalDate.ofEpochDay(ndate.get.getDaysSinceEpoch.toLong))
//   // var diff: Option[Int] = cdiff
//
//   override def toString(): String = "Tranx(%s, %s) %s - %s : %s, ldate:%s, ndate:%s, diff:%s".format(vdate, uid, c1, sid, payment, vldate, vndate, diff)
// }

case class Tranx(uid: String, date: Date, c1: String, c2: String, c3: String, sid: String, payment: Double)

class SparkApp extends BaseApp {
  // def getSc() : SparkContext = {
  //   val mysc = BaseApp().sc
  //   return mysc
  // }
//  def sc = BaseApp().sc
  def sqlContext = new SQLContext(sc)
}

/*
import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
import java.time.LocalDate
import org.sws9f.sparkcalc._

val myapp = new SparkApp()
val sc = myapp.getSc()
val sqlc = new org.apache.spark.sql.SQLContext(sc)
import sqlc.implicits._

sqlc.sql(
   """CREATE TEMPORARY TABLE trans
     |USING org.apache.spark.sql.cassandra
     |OPTIONS (
     |  table "trans",
     |  keyspace "ks1",
     |  cluster "Test Cluster",
     |  pushdown "true",
     |  spark.cassandra.input.split.size_in_mb 256
     |)""".stripMargin)

val df = sqlc.sql("SELECT * FROM trans WHERE date = '2014-04-22' LIMIT 1000")
df.cache
df.registerTempTable("trans422")
val df2 = sqlc.sql("SELECT c1, sid, COUNT(uid) AS tranx, AVG(diff) AS avg, STDDEV_POP(diff) AS std FROM trans422 t GROUP BY t.c1, t.sid")
df2.cache

val rdd1 = sc.cassandraTable("ks1", "trans").select("uid").take(10).toArray.foreach(println)

val rdd2 = sc.cassandraTable[Tranx]("ks1", "trans").where("date = ?", "2014-02-24").take(10).toArray.foreach(println)
rdd2.count

import java.sql.Date
case class TTT ( uid: String, sid:String, diff: Option[Int], date:Date )

class TTT2 ( var uid: String, var sid:String, var diff: Option[Int] )

val rddRaw = sc.cassandraTable[Tranx]("ks1", "trans").where("date = ?", "2014-02-24")
rddRawTrans.registerTempTable("raw_trans")

var rdd4 = sc.cassandraTable[TTT]("ks1", "trans").select("uid", "date").where("date = ?", "2014-02-24")

val rdd4 = sc.cassandraTable[TTT]("ks1", "trans").select("uid", "sid", "diff", "date").where("date = ?", "2014-02-24")
val df4 = rdd4.toDF()

val df4 = sc.cassandraTable[TTT]("ks1", "trans").select("uid", "sid", "diff", "date").where("date = ?", "2014-02-24").toDF()
val df5 = sc.cassandraTable[Trans]("ks1", "trans").where("date <= ?", "2014-02-24").toDF()

val df5 = sc.cassandraTable[Trans]("ks1", "trans").toDF()
val df5A = df5.filter(df5("date") < "2014-02-24")
df5A.repartition(8)
df5A.registerTempTable("df5")



val df6 = sqlc.sql("SELECT c1, sid, COUNT(uid) AS tranx, AVG(diff) AS avg, STDDEV_POP(diff) AS std FROM df5 t GROUP BY t.c1, t.sid")

val df4 = sc.cassandraTable[TTT2]("ks1", "trans").select("uid", "sid", "diff", "date").where("date = ?", "2014-02-24").toDF()


case class Tranx2(
  uid: String,
  date: LocalDate,
  c1: String, c2: String, c3: String, sid: String, payment: Double,
  ldate: Option[LocalDate],
  ndate: Option[LocalDate],
  diff: Option[Int])




val people = sc.textFile("examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
people.registerTempTable("people")



val myrdd = sc.cassandraTable[Tranx]("ks1", "trans")

val df = sqlc.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "trans", "keyspace" -> "ks1")).load()
df.filter("date = '2014-04-25'").show()



sc.stop

// use c1, sid as partition key, then rebuild the process
// spark.sql GROUP BY N >> N stage?
*/