package com.cnsuning.sngm.SparkStreaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * name:KafkaDirectWordCount
  * description: example of connect kafka and process data with spark streaming
  * running mode:Appended by daily frequency
  * target:SNGMSVC.
  * createdate:2019/2/20
  * author:18040267
  * email:ericpan24@gmail.com
  * copyRight:Suning
  * modifyby:
  * modifydate:
  */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[3]")
    val ssc = new StreamingContext(conf,Seconds(5))
//    创建一份kafka参数map
    val kafkaParams = Map(("group.id","yingyanxg_pre_topic2_group1"),
      ("metadata.broker.list","kafkaprexg01broker01.cnsuning.com:9092,kafkaprexg01broker02.cnsuning.com:9092,kafkaprexg01broker03.cnsuning.com:9092"))
    val topic =Set("yingyanxg_pre_topic2")

    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topic).map(_._2)
    val word = lines.flatMap(_.split(";"))
    val pairs = word.map((_,1))
    //    val wordCount = pairs.reduceByKey(_ + _)
    pairs.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
