package com.cnsuning.sngm.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *
  */
object StreamingStartDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StreamingFirst").setMaster("local")
    val ssc = new StreamingContext(conf,Seconds(1))
    ssc.sparkContext.setLogLevel("WARN")
    val lines = ssc.socketTextStream("localhost",7777)
    val errorLines = lines.filter(_.contains("error"))
    errorLines.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
