package org.sws9f.sparksample

import com.datastax.driver.core.{Cluster, Host, Metadata, Row, Session, ResultSet, BoundStatement}
import org.apache.cassandra.io.sstable.CQLSSTableWriter
import scala.collection.JavaConversions._
import com.github.tototoshi.csv._
import java.util.Date


object S06CaxBulkLoad extends DemoApp {
  
  case class Tranx(uid: String, date: Date, c1: String, c2: String, c3: String, sid: String, payment: Double, ldate: Date, ndate: Date, diff: Option[Int])
  
  val csvFile = "/Users/larrysu/repos/data/trans_1k.csv"
  
  //CREATE KEYSPACE ks1 WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
  val schema = """CREATE TABLE ks1.trans (
                 |  uid varchar, date varchar,
                 |  c1 varchar, c2 varchar, c3 varchar, sid varchar,
                 |  payment varchar, ldate varchar, ndate varchar, diff varchar,
                 |  PRIMARY KEY ((date), uid, c1, c2, c3, sid)
                 |);
                 |""".stripMargin
  val insert = "INSERT INTO ks1.trans (uid, date, c1, c2, c3, sid, payment, ldate, ndate, diff) VALUES (?,?,?,?,?,?,?,?,?,?);"

  println("SCHEMA: \n" + schema)

  val writer: CQLSSTableWriter = CQLSSTableWriter.builder()
                                                .inDirectory("/Users/larrysu/repos/data/sstable")
                                                .forTable(schema)
                                                .using(insert).build();

  val reader = CSVReader.open(csvFile)
  val formatDate = new java.text.SimpleDateFormat("yyyy-MM-dd")
  var line = reader.readNext()
  var lineCount: Long = 1

  val cqlInsert = "INSERT INTO ks1.trans (uid, date, c1, c2, c3, sid, payment, ldate, ndate, diff) VALUES (?,?,?,?,?,?,?,?,?,?);"

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
    
    println(lineCount + " : " + t)

    writer.addRow(
      t.uid,
      t.date, t.c1, t.c2, t.c3, t.sid,
      t.payment.asInstanceOf[java.lang.Double],
      t.ldate,
      t.ndate,
      if (t.diff == None) null else t.diff.get.asInstanceOf[java.lang.Integer] )

    lineCount += 1
    line = reader.readNext()

  }

  writer.close();
                 
}

