package org.sws9f.sparkcalc

import org.apache.cassandra.io.sstable.CQLSSTableWriter
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import com.github.tototoshi.csv._
import java.time.LocalDate
import scalax.file.Path
import com.typesafe.scalalogging.LazyLogging

object CaxSSTableWriterTranx extends App with LazyLogging {
  
  val conf = ConfigFactory.load()
  val settingSchema = conf.getString("cax-db.schema").stripMargin
  val settingInsertStatement = conf.getString("cax-db.insert-statement").stripMargin
  val settingSStableFolder = conf.getString("cax-db.sstable-folder")
  val settingSourceCsv = conf.getString("cax-db.source-csv")
  
  logger.info("Using Schema:" + settingSchema)
  logger.info("Using Insert Statement:" + settingInsertStatement)
  logger.info("Source CSV: " + settingSourceCsv)
  logger.info("Destination Folder: " + settingSStableFolder)
  
  val destPath: Path = Path.fromString(settingSStableFolder)
  if (destPath.exists) {
    logger.info("Folder %s exists, deleting it.".format(settingSStableFolder))
    try {
      destPath.deleteRecursively(continueOnFailure = false)
    } catch {
      case e: java.io.IOException => println("cannot delete file: " + e)
    }
  }
  destPath.createDirectory()
  
  case class Tranx(uid: String, date: LocalDate, c1: String, c2: String, c3: String, sid: String, payment: Double)

  val writer: CQLSSTableWriter = CQLSSTableWriter.builder()
                                                .inDirectory(settingSStableFolder)
                                                .forTable(settingSchema)
                                                .using(settingInsertStatement).build();

  val reader = CSVReader.open(settingSourceCsv)
  var line = reader.readNext()
  var lineCount: Long = 0
  val epochMiddle = scala.math.pow(2,31).toInt + 1

  while(line != None) {
    val t = Tranx(
      line.get(0),
      LocalDate.parse(line.get(1)),
      line.get(2),
      line.get(3),
      line.get(4),
      line.get(5),
      line.get(6).toDouble
    )

    lineCount += 1
    if (lineCount % 100000 == 0) {
//    if (lineCount % 10000 == 0) {      
      logger.info(lineCount + " : " + t)
    }
    
    val idate : java.lang.Integer = t.date.toEpochDay().toInt + epochMiddle

    writer.addRow(
      t.uid,
      idate,
      t.c1, t.c2, t.c3, t.sid,
      t.payment.asInstanceOf[java.lang.Double]
    )
    line = reader.readNext()
  }

  logger.info("Processed %s lines".format(lineCount))
  reader.close();
  writer.close();
                 
}

