package com.cnsuning.sngm

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


/**Program used to deal with Activity Management List module(AML), on daily frequency
  *
  *
  */

// different between spark and scala timestamp
//spark 的unix时间戳 只到秒，scala 到 毫秒
object timeTest {
  def main(args: Array[String]): Unit = {
    /*接收并处理时间参数*/
    val sparkConf = new SparkConf().setAppName("selfTestDFmap")
      .setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//    val sc = spark.sqlContext
    val data:DataFrame = spark.createDataFrame(Seq(
      ("1", "20181213", "杭州市"),
      ("2", "20150505 场店", "苏州市"),
      ("3", "20190109  广场店", "南京州")
    )).toDF("id","statis_date","cityName")
    val data2 = data.withColumn("statis_date1",unix_timestamp(data.col("statis_date"),"yyyyMMdd"))
    data2.show()
    val maxDate:Seq[Any] = data2.select("statis_date1").groupBy().max().head.toSeq
    val a = maxDate
    println(maxDate)

    val now = new Date()
    val date1 = "20180401"
    val date2 = new SimpleDateFormat("yyyyMMdd")
    val executeDate = date2.parse(date1).getTime/1000
    val data3 = data2.withColumn("state",when(data2.col("statis_date1") > executeDate,1).when(data2.col("statis_date1") < executeDate,2).otherwise(0))
    println(date2.format(now))


    println(executeDate)
    data3.show()
  }
}
