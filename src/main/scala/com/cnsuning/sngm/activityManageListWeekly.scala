package com.cnsuning.sngm

/**
  *
  *
  */
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object activityManageListWeekly {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("selfTestDFmap")
      .setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val tu = ("4","北京大\t红\n门店","北京市")
    val sc = spark.sqlContext
    val data:DataFrame = sc.createDataFrame(Seq(
      ("1", "杭州市西溪广场店", "杭州市"),
      ("2", "苏州中心广 场店", "苏州市"),
      ("3", "南京玄武区中心  广场店", "南京州"),
      tu
    )).toDF("id","storeName","cityName")//(ExpressionEncoder(): Encoder[Seq(String, String,String)])

    data.show()
    val data1 = data.withColumn("storeName",regexp_replace(data.col("storeName"),"\t|\n|\r|\\s+|区|市",""))
//        .withColumn()
    data1.show()
  }
}
