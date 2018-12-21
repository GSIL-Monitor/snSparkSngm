package com.cnsuning.sngm

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**https://stackoverflow.com/questions/36784735/how-to-flatmap-a-nested-dataframe-in-spark
  *
  *
  */
object dataFrameToDataSet {

  case class Info(statis_date:String,id:Int,city_nm:String)
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("dfToDs").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val dataFrame:DataFrame = spark.createDataFrame(Seq(
      (1, "20181213", "杭 州 市"),
      (2, "20181213", "苏 州 市"),
      (3, "20190109", "南 京 市")
    )).toDF("id","statis_date","city_nm")

    dataFrame.show()
    import spark.implicits._
    val  dataSet = dataFrame.as[Info]
    dataSet.show()
    dataSet.drop("statis_date").show()

    val dataSet2 = dataFrame.as[(Int,String,String)]
    println("==================" + dataSet2.getClass)
    dataSet2.show()

    val dataSet3 = dataSet2.flatMap{
      case (id,statis_date,city_nm) => city_nm.split(" ").map((id,statis_date,_))
    }.toDF("id","statis_date","city_nm").as[Info]
    println("--------------00000000000000----------")
    dataSet3.show()
//    val data = dataSet.flatMap()
//    data.show()

//    val s = for(i <- 1 to 10 by 2 )
//      yield i.toString
//
//    val ss = s.map(("aaa",_))
//    println(s.getClass)
//    println(s)
//    println(ss.getClass)
//    println(ss)

    println("=====================汇聚==============")
    val dataSet4 = dataSet3.groupBy("city_nm").sum("id").as("sum_id")
    dataSet4.show()
    val dataSet5 = dataSet3.groupBy("city_nm").agg(count("statis_date").as("date_cnt"),sum("id").as("id_sum"))
    dataSet5.show()

//    val dataFrame1 = dataSet3

//    val gg1 = Grouping(dataFrame).set("gggg")
//    gg1.show()

  }
}
