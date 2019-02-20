package com.cnsuning.sngm.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * name:KafkaSparkStreaming
  * description: example of connect kafka and process data with spark streaming
  * running mode:Appended by daily frequency
  * target:SNGMSVC.
  * createdate:2019/2/19
  * author:18040267
  * email:ericpan24@gmail.com
  * copyRight:Suning
  * modifyby:
  * modifydate:
  */
object KafkaReceiverWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[3]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val topicMap = Map("yingyanxg_pre_topic2" -> 1)
    val lines = KafkaUtils.createStream(ssc,
      "kafkaprexg01zk01.cnsuning.com:2181,kafkaprexg01zk02.cnsuning.com:2181,kafkaprexg01zk03.cnsuning.com:2181",
      "yingyanxg_pre_topic2_group1",
      topicMap).map(_._2)

    val word = lines.flatMap(_.split(";"))
    val pairs = word.map((_,1))
//    val wordCount = pairs.reduceByKey(_ + _)
    pairs.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
