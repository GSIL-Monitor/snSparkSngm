package com.cnsuning.sngm

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object caseClassCreateDataSet {
  case class Info(statis_date:String,id:Int,city_nm:String)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dfToDs").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

//    val dataFrame:Dataset = spark.createDataset(Seq(
//      (1, "20181213", "杭 州 市"),
////      (2, "20181213", "苏 州 市"),
////      (3, "20190109", "南 京 市")
////    ))
//    import spark.implicits._
//    val ds:Dataset = spark.createDataset(Seq((1, "20181213", "杭 州 市"),(2, "20181213", "苏 州 市"))).as[Info]
  }
}
