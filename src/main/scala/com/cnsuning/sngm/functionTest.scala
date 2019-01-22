package com.cnsuning.sngm

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * name:functionTest
  * description: NoDetail
  * running mode:Appended by daily frequency
  * target:SNGMSVC.
  * createdate:2019/1/22
  * author:18040267
  * email:ericpan24@gmail.com
  * copyRight:Suning
  * modifyby:
  * modifydate:
  */
object functionTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dfToDs").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df:DataFrame = spark.createDataFrame(Seq(
      (1, "20181213", "杭 州 市"),
      (2, "20181213", "苏 州 市"),
      (3, "20190109", "南 京 市")
    )).toDF("id","statis_date","city_nm")

    val d = df.select(col("id"),lit(99).as("ss"),lit(95).as("sd"),col("city_nm"))
        .withColumn("aaaa",col("ss")-col("sd"))
//    d.show()
    val d2 =df.count()
    print(d2)
  }
}
