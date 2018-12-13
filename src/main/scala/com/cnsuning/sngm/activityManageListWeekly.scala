package com.cnsuning.sngm

/**
  *
  *
  */
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql._
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
    val df1 = sc.createDataFrame(Seq(
      ("1",2,11,3),
      ("2",3,5,5),
      ("3",6,8,7)
    )).toDF("id","v1","v2","v3")
    data.show()
    val now = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    val data1 = data.withColumn("storeName",regexp_replace(data.col("storeName"),"\t|\n|\r|\\s+|区|市",""))
        .withColumn("etl_time",lit(date))
//        .withColumn()
    data1.show()
    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
//    df2 = df1.
      df1.show()
//    val df2 = df1.withColumn("v3",max("v2"))
//    df2.show()
//    val maxCols: Array[Column] = df1.columns.map(max)
    val seqMax: Seq[Any] = df1.groupBy().max().head.toSeq
    val seqMin: Seq[Any] = df1.groupBy().min().head.toSeq
//    val df2 = df1.select(col("id"),col("v2"))
    var vmax:Int=seqMax(0).asInstanceOf[Int]
    var vmin:Int=seqMin(0).asInstanceOf[Int]
    for (i <- seqMax) if (i.isInstanceOf[Int]) vmax = if(vmax < i.asInstanceOf[Int]) i.asInstanceOf[Int] else vmax
    for (i <- seqMin) if (i.isInstanceOf[Int]) vmin = if(vmin > i.asInstanceOf[Int]) i.asInstanceOf[Int] else vmin

    println("max----------" + vmax + "min----------" + vmin)
//    df2.show()
//    val seq :Seq[Int] = seqMax.foreach(x => )
    println(seqMax + "------" + seqMin)


    val tm:Long = 2147483646000L
    val tm1:String = "2017-08-01 16:44:32"
    val fm:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val fm1:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm1.parse(tm1)
    val t =fm.format(tm)
    println(t.getClass)
//    println(seq)


  }
}
