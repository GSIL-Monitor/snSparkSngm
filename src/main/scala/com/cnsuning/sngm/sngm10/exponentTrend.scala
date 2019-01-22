package com.cnsuning.sngm.sngm10

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * name:exponentTrend
  * description: Designed for sngm version 10 ,
  *              run this program to get the data of tendency chart,
  *              which placed at top of the mobile client consume_insight page.
  * running mode:Appended by daily frequency
  * target:SNGMSVC.T_MOB_TREND_EXPONENT_D
  * createdate:2019/1/21
  * author:18040267
  * email:ericpan24@gmail.com
  * copyRight:Suning
  * modifyby:
  * modifydate:
  */
object exponentTrend {

  /**
    * this function used to get the maximum & minimum pay amount at every city's store type,distance of store with res limited in 5/3/1 kilometer.
    * */
  def produceLastYearExtremum(curYr:String,lstYr:String,spark:SparkSession)={
//    define query sentence
    val queryStrResMap = "select city_cd,str_type,str_cd,res_cd,distance from sospdm.sngm_store_res_map t where city_cd='025'"
    val queryPayByStr = "select statis_date,city_code city_cd,res_cd,str_cd,pay_amnt from sospdm.sngm_t_order_width_07_d where " +
                        "statis_date <='" + curYr + "0101' and statis_date >= '" + curYr + "0101' and city_code='025'"

//    do lazy query and transform
    val dfOriginalResMap = spark.sql(queryStrResMap)
    val dfOriginalStrPay = spark.sql(queryPayByStr)
    val df1 = dfOriginalResMap.filter(dfOriginalResMap.col("distance") <= 5)//.withColumn("distance",lit("5km"))
    val df2 = df1.filter(df1.col("distance") <= 1)//.withColumn("distance",lit("1km"))

//    do summary by store res per day
    val dfPay1 = dfOriginalStrPay.groupBy("statis_date","city_cd","res_cd","str_cd").agg(sum("pay_amnt").as("pay_amnt"))

//    get summary pay amount of 5 km store-res mapping ,then do summary to get store pay amount at every day.
//    And then get maximum pay amount at whole year of every city and str_type.
    val dfPayMaxPerCity =
      df1.join(dfPay1,Seq("city_cd","str_cd","res_cd"),"inner")
        .groupBy("statis_date","city_cd","str_type","str_cd").agg(sum("pay_amnt").as("pay_amnt"))
        .groupBy("city_cd","str_type").agg(max("pay_amnt").as("pay_max_amnt"))
        .withColumn("pay_min_amnt",lit(0))

//    same approach as maximum to get minimum
    val dfPayMinPerCity =
      df2.join(dfPay1,Seq("city_cd","str_cd","res_cd"),"inner")
        .groupBy("statis_date","city_cd","str_type","str_cd").agg(sum("pay_amnt").as("pay_amnt"))
        .groupBy("city_cd","str_type").agg(min("pay_amnt").as("pay_min_amnt"))
        .select(col("city_cd"),col("str_type"),lit(0).as("pay_max_amnt"),col("pay_min_amnt"))


    val dfLstYrExtrm = dfPayMaxPerCity.union(dfPayMinPerCity).groupBy("city_cd","str_type")
      .agg(sum("pay_max_amnt").as("pay_amnt_max"),sum("pay_min_amnt").as("pay_amnt_min"))
        .withColumn("pay_amnt_delta",col("pay_amnt_max")-col("pay_amnt_min"))

    spark.sql("use sngmsvc")
    dfLstYrExtrm.createOrReplaceTempView("lstyrextrm")
    spark.sql("insert overwrite table SNGMSVC.T_MOB_FULLYEAR_EXTREMUN partition(year='"+lstYr+"') " +
      " select city_cd,str_type,pay_amnt_max,pay_amnt_min,pay_amnt_delta from lstyrextrm")
//    dfStrResMap.write.mode("append").partitionBy("year").saveAsTable("tblnm") // confirm table format should be parquet
//    dfStrResMap.createOrReplaceTempView("strResMapTMP")

    //    dfStrResMap.write.mode("overwrite").saveAsTable("sngmsvc.sngm_exponent_trend_str_map")

//    dfStrResMap.createOrReplaceTempView("tmp")

  }


  // main function
  def main(args: Array[String]): Unit = {
  // receive parameter from suning IDE dispatcher
  val statis_date = args(0) // current date yyyyMMdd
  val lstYr = args(1) //last year
  val curYr = args(2) //current year
  // define spark session
  val sc = new SparkConf().setAppName("exponentTrend")//.setMaster("local")
      .set("spark.sql.hive.metastorePartitionPruning", "false")
  val spark = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()

  // create temp table
  spark.sql("use sngmsvc")
  spark.sql("""CREATE TABLE IF NOT EXISTS SNGMSVC.T_MOB_FULLYEAR_EXTREMUN (
              CITY_CD STRING COMMENT '城市编码',
              STR_TYPE_CD STRING COMMENT '门店业态编码',
              PAY_AMNT_MAX DECIMAL(17,2) COMMENT '消费最高值',
              PAY_AMNT_MIN DECIMAL(17,2) COMMENT '消费最低值',
              PAY_AMNT_DELTA DECIMAL(17,2) COMMENT '消费值极差'
              ) partitioned by (year int comment '年' )
              stored as rcfile""")

// when the number of extremum at NanJing(025) less then 3 ,it's means table has been truncated and should be update.
  val querySql = "select * from SNGMSVC.T_MOB_FULLYEAR_EXTREMUN where year = '"+lstYr+"'"
  val dfExtrm = spark.sql(querySql)
  if(dfExtrm.filter(col("city_cd")==="025").count() < 5){
    produceLastYearExtremum(curYr,lstYr,spark)
  }

  val sdf = "asdf"
  }
}
