package com.cnsuning.sngm.sparkDemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * name:InitSparkSession
  * description: initialize spark session for multiple object's using with convenient
  * running mode:Appended by daily frequency
  * target:SNGMSVC.
  * createdate:2019/2/12
  * author:18040267
  * email:ericpan24@gmail.com
  * copyRight:Suning
  * modifyby:
  * modifydate:
  */
object InitSpark {
  def apply(appName:String,url:String):SparkSession = {
   val is = new InitSpark(appName,url)
    is.ss
  }
}

class InitSpark(val appName:String,val url:String) {
  val sc = new SparkConf().setAppName(appName)
    .setMaster(url)
    .set("spark.sql.hive.metastorePartitionPruning", "false")
  val spark = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()
  def ss = this.spark
}
