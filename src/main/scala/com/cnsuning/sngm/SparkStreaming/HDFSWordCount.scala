package com.cnsuning.sngm.SparkStreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/**
  * name:HDFSWordCount
  * description: NoDetail
  * running mode:Appended by daily frequency
  * target:SNGMSVC.
  * createdate:2019/2/18
  * author:18040267
  * email:ericpan24@gmail.com
  * copyRight:Suning
  * modifyby:
  * modifydate:
  */
object HDFSWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HDFSWordCount").setMaster("local[3]")
    val ssc = new StreamingContext(conf,Seconds(2))

  }

}
