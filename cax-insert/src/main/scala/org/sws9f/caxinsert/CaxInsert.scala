package org.sws9f.caxinsert

import com.datastax.driver.core.{Cluster, Host, Metadata, Row, Session, ResultSet, BoundStatement}
import scala.collection.JavaConversions._
import java.util.Date
import java.time.LocalDate

object CaxInsert extends App {
  val client = new SimpleClient()
  client.connect("127.0.0.1")
  client.insertData()
  client.close()
}

class SimpleClient {
   var cluster: Cluster = null
   var session: Session = null
   
   case class Test2Row(s: String, d: Date, i: Int, d2: LocalDate)

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
     val cqlInsert = "INSERT INTO ks1.test2 (s, d, i, d2) VALUES (?,?,?,?);"
     val stat = session.prepare(cqlInsert)
     val boundStat = new BoundStatement(stat)
     val r = Test2Row("abcx", new Date(), 123, LocalDate.parse("2016-03-03"))
     val cql = boundStat.bind(r.s, r.d, r.i.asInstanceOf[java.lang.Integer], 
       com.datastax.driver.core.LocalDate.fromDaysSinceEpoch(r.d2.toEpochDay().toInt)
     )
     session.execute(cql)
   }
}