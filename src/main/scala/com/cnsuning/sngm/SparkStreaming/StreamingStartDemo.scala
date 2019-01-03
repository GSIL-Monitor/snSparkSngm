package com.cnsuning.sngm.SparkStreaming

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *
  */
object StreamingStartDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StreamingFirst").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(1))
    ssc.sparkContext.setLogLevel("WARN")

    val lines = ssc.socketTextStream("127.0.0.1",7777) //nc -L -p 777
    val errorLines = lines.filter(_.contains("Failed"))
    errorLines.print()
//    lines.print(10)
    ssc.start()
    ssc.awaitTermination()

//    val socket = new Socket("127.0.0.1",7777)
//    val inputStream = new ObjectInputStream(socket.getInputStream)
//    while(true){
//      println(inputStream.readObject())
//    }
  }
}
