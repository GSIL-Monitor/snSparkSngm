package com.cnsuning.sngm

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object testGrouping {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local") .setAppName("testGrouping")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val dataFrame:DataFrame = spark.createDataFrame(Seq(
      ("CHN", "10001", "025",10),
      ("CHN", "10001", "021",10),
      ("CHN", "10001", "022",10),
      ("CHN", "10002", "010",10),
      ("CHN", "10001", "025",10),
      ("CHN", "10002", "011",10),
      ("USA", "10001", "025",10),
      ("USA", "10001", "025",10),
      ("USA", "10002", "001",10),
      ("USA", "10002", "001",10)
    )).toDF("state","area_cd","city_cd","pay_amnt")

    import spark.implicits._
    val dataSet = dataFrame.as[(String,String,String,Int)]
    dataSet.groupBy("state","area_cd","city_cd").agg(sum("pay_amnt").as("sum_amnt"),count("pay_amnt").as("cnt_amnt")).show()


    val seq1 = Seq(
      ("amnt_sum1" , "sum" ,"pay_amnt"),
      ("amnt_sum2" , "sum" ,"pay_amnt"),
      ("amnt_sum3" , "sum" ,"pay_amnt"),
      ("amnt_sum4" , "sum" ,"pay_amnt"),
      ("amnt_cnt" , "count","pay_amnt")
    )


    val dsG = Grouping(spark,dataSet,Array("state","area_cd","city_cd"),seq1)
    dsG.show
    val d =dsG.groupby()
    println(d.getClass)
    d.show
    val e = d.set(Array("state","area_cd"))
//    e.sh

//    val a=(String,String,Int)
//    println(a.getClass)



  }
}
