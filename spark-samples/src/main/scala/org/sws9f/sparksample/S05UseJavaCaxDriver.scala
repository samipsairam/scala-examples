package org.sws9f.sparksample

import com.datastax.driver.core.{Cluster, Host, Metadata, Row, Session, ResultSet, BoundStatement}
import scala.collection.JavaConversions._
import com.github.tototoshi.csv._
import java.util.Date

object S05UseJavaCaxDriver extends DemoApp {
  val client = new SimpleClient()
  client.connect("127.0.0.1")
  client.insertData()
  client.close()
}

class SimpleClient {
   var cluster: Cluster = null
   var session: Session = null
   val csvFile = "/Users/larrysu/repos/data/trans_1m.csv"
   
   case class Tranx(uid: String, date: Date, c1: String, c2: String, c3: String, sid: String, payment: Double, ldate: Date, ndate: Date, diff: Option[Int])

   def connect(node: String) {
      cluster = Cluster.builder()
            .addContactPoint(node)
            .build()
      val metadata = cluster.getMetadata()
      println("Connected to cluster: %s\n".format(metadata.getClusterName() ))
      
      metadata.getAllHosts().foreach(
        (h: Host) => println("Datatacenter: %s; Host: %s; Rack: %s".format( h.getDatacenter(), h.getAddress(), h.getRack() ))
      )
      
      session = cluster.connect()
   }

   def close() {
      session.close()
      cluster.close()
   }
   
   def insertData() {
     val reader = CSVReader.open(csvFile)
     val formatDate = new java.text.SimpleDateFormat("yyyy-MM-dd")
     val formatUtc = new java.text.SimpleDateFormat("yyyy-MM-dd+0000")
     var line = reader.readNext()
     var lineCount: Long = 1

     val cqlInsert = "INSERT INTO ks1.trans2 (uid, date, c1, c2, c3, sid, payment, ldate, ndate, diff) VALUES (?,?,?,?,?,?,?,?,?,?);"
     val stat = session.prepare(cqlInsert)
     val boundStat = new BoundStatement(stat)
     
     while(line != None) {
       val t = Tranx(
         line.get(0),
         formatDate.parse(line.get(1)),
         line.get(2),
         line.get(3),
         line.get(4),
         line.get(5),
         line.get(6).toDouble,
         if (line.get(7).length == 0) null else formatDate.parse(line.get(7)),
         if (line.get(8).length == 0) null else formatDate.parse(line.get(8)),
         if (line.get(9).length == 0) None else Some(line.get(9).toInt)
       )

       if (lineCount % 500 == 0) {
         println(lineCount + " : " + t)
       }

       val cql = boundStat.bind(
         t.uid,
         t.date, t.c1, t.c2, t.c3, t.sid,
         t.payment.asInstanceOf[java.lang.Double],
         t.ldate,
         t.ndate,
         if (t.diff == None) null else t.diff.get.asInstanceOf[java.lang.Integer] )

       // println(lineCount + " CQL: " + cql)
       session.execute(cql)

       lineCount += 1
       line = reader.readNext()

     }

     
     // val cqlInsert = "INSERT INTO ks1.trans (uid, date, c1, c2, c3, sid, payment, ldate, ndate, diff) VALUES ('%s','%s','%s','%s','%s','%s',%s, %s, %s, %s)"
     // while(line != None) {
     //   val t = Tranx(
     //     line.get(0),
     //     formatDate.parse(line.get(1)),
     //     line.get(2),
     //     line.get(3),
     //     line.get(4),
     //     line.get(5),
     //     line.get(6).toDouble,
     //     if (line.get(7).length == 0) null else formatDate.parse(line.get(7)),
     //     if (line.get(8).length == 0) null else formatDate.parse(line.get(8)),
     //     if (line.get(9).length == 0) None else Some(line.get(9).toInt)
     //   )
     //
     //   if (lineCount % 500 == 0) {
     //     println(lineCount + " : " + t)
     //   }
     //
     //   val cql = cqlInsert.format(
     //     t.uid,
     //     formatUtc.format(t.date), t.c1, t.c2, t.c3, t.sid,
     //     t.payment,
     //     if (t.ldate == null) "null" else "'" + formatUtc.format(t.ldate) + "'",
     //     if (t.ndate == null) "null" else "'" + formatUtc.format(t.ndate) + "'",
     //     if (t.diff == None) "null" else t.diff.get )
     //
     //   // println(lineCount + " CQL: " + cql)
     //   session.execute(cql)
     //
     //   lineCount += 1
     //   line = reader.readNext()
     //
     // }
     
     
     reader.close()
   }
}