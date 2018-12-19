package com.cnsuning.sngm

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

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
      (2, "20150505", "苏 州 市"),
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
//
//    val dataSet3 = dataSet2.flatMap{
//      case (id,statis_date,city_nm) => city_nm.split(" ").map((id,statis_date,_))
//    }.toDF("statis_date","id","city_nm").as[Info]
//    println("--------------00000000000000----------")
//    dataSet3.show()
//    val data = dataSet.flatMap()
//    data.show()
  }
}
