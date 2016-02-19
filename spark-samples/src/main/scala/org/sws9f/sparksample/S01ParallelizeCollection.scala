package org.sws9f.sparksample

import org.apache.spark.{Logging, SparkContext, SparkConf}

object S01ParallelizeCollection extends DemoApp {
  val mysc = DemoApp().sc
  val data = Array(1,2,3,4,5)
  val distData = mysc.parallelize(data)
  var sum = distData.reduce( (a, b) => a + b )
  
  println("Result of sum: %s".format(sum))
  mysc.stop()
}
