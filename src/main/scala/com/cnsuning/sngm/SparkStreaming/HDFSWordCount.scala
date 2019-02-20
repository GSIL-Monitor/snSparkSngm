package com.cnsuning.sngm.SparkStreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

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
    val path:String = args(0)
    val conf = new SparkConf().setAppName("HDFSWordCount").setMaster("local[3]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val LOGGER = LoggerFactory.getLogger(HDFSWordCount.getClass)
    val path1 = "/user/sospdm/hive/warehouse/sospdm.db/test_pxz/streaming"
    LOGGER.info("================HDFS===============:{}",path)
    LOGGER.info("================HDFS1===============:{}",path1)

    val lines = ssc.textFileStream(path1)
    val word = lines.flatMap(_.split(" "))
    val pairs = word.map((_,1))
    val wordCount = pairs.reduceByKey(_ + _)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
