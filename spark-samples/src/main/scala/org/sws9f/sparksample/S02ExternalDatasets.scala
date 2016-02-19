package org.sws9f.sparksample

import org.apache.spark.{Logging, SparkContext, SparkConf}

object S02ExternalDatasets extends DemoApp {
  val mysc = DemoApp().sc
  val distFile = mysc.textFile(textFilePath, 10)
  
  println("Result Count: %s".format(distFile.count()))
  println("Result first 10 lines:")
  distFile.take(10).foreach(println)
  
  val sizeOfAllLines = distFile.map(s => s.length).reduce((a, b) => a + b)
  println("sizeOfAllLines: %s".format(sizeOfAllLines))
  
  mysc.stop()
}
