package com.cnsuning.sngm.sngm10

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * name:cumulateExponent
  * description: Designed for sngm version 10,
  *             run this program to get the data of main consume exponent and the growth rate
  * running mode:Appended by daily frequency
  * target:SNGMSVC.T_MOB_CUMULATE_EXPONENT_D
  * createdate:2019/1/25
  * author:18040267
  * email:ericpan24@gmail.com
  * copyRight:Suning
  * modifyby:
  * modifydate:
  */
object cumulateExponent {

  /*
  *  this function used to get maximum & minimum cumulate pay amount on 7/15/30 days
  *  at every city's store type,distance of store with res limited in 5/3/1 kilometer.
  * */
  def produceGivenDurationExtremum(statis_date:String,Duration:Int,spark:SparkSession){
    spark.sql("use sngmsvc")
//    define query sentence
    val queryStrResMap = "select city_cd,str_type,str_cd,res_cd,distance from sospdm.sngm_store_res_map t where city_cd = '025'"
    val queryPayByStr = "select statis_date,city_code city_cd,res_cd,str_cd,pay_amnt rom sospdm.sngm_t_order_width_07_d where " +
      "statis_date <='"+statis_date+"' and statis_date >'"+DateUtils(statis_date,"yyyyMMdd",Duration-30)+"' and city_cd = '025'"
//    do lazy query and transform
    val dfOriginalResMap = spark.sql(queryStrResMap)
    val dfOriginalStrPay = spark.sql(queryPayByStr)
    val df1 = dfOriginalResMap.filter(col("distance")<=5)
    val df2 = df1.filter(col("distance")<=1)

//    do summary by store res per day
    val dfPay = dfOriginalStrPay.groupBy("statis_date","city_cd","res_cd","str_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))

//    get summary pay amount of 5 km store-res mapping ,then do summary to get store pay amount at every day.
    val dfPay5kmPerStr = df1.join(dfPay,Seq("city_cd","str_cd","res_cd"),"inner")
      .groupBy("statis_date","city_cd","str_type","str_cd").agg(sum("pay_amnt").as("pay_amnt"))

//    same approach to get 1 km
    val dfPay1kmPerStr = df2.join(dfPay,Seq("city_cd","str_cd","res_cd"),"inner")
      .groupBy("statis_date","city_cd","str_type","str_cd").agg(sum("pay_amnt").as("pay_amnt"))

//  put dfPay5km & dfPay1km to hive temproray table ,use hive's advance function
    dfPay5kmPerStr.createOrReplaceTempView("dfPay5kmPerStr"+Duration.toString)
    dfPay1kmPerStr.createOrReplaceTempView("dfPay1kmPerStr"+Duration.toString)
    dfPay5kmPerStr.write.mode("overwrite").saveAsTable("sngmsvc.t_mob_duration_extremum_tmp")

//    val dfQuantile = spark.sql("select city_cd,str_type," +
//      "percentile_approx(pay_amnt,0.75) pay_amnt_75,percentile_approx(pay_amnt,0.25) pay_amnt_25,min(pay_amnt) pay_amnt_min " +
//      "from ( " +
//            "select statis_date,city_cd,str_type,str_cd,pay_amnt " +
//            "from ( " +
//                    "select statis_date,city_cd,str_type,str_cd,sum(pay_amnt) pay_amnt from dfPay1kmPerStr"+Duration.toString +
//                    " group by city_cd,str_type,str_cd,"+
//                  ") t where statis_date >'"+DateUtils(statis_date,"yyyyMMdd",-30)+"' " +
//      ") t group by city_cd,str_type ")


  }
  /*
  *  main function
  * */
  def main(args: Array[String]): Unit = {
//    receive parameter from suning IDE dispatcher
    val statis_date = args(0)
//    val lstMon = args(1)
//    define spark session
    val sc = new SparkConf().setAppName("cumulateExponent")
      .set("spark.sql.hive.metastorePartitionPruning", "false")
    val spark = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()
//    create temp table
    spark.sql("use sngmsvc")
    spark.sql(
      """CREATE TABLE IF NOT EXISTS SNGM.T_MOB_CUMULATE_EXTREMUM(
        CITY_CD STRING COMMENT '城市编码',
        STR_TYPE STRING COMMENT '门店业态编码',
        CUMULATE_DAYS STRING COMMENT '累计天数',
        PAY_AMNT_MAX DECIMAL(17,2) COMMENT '消费最高值',
        PAY_AMNT_MIN DECIMAL(17,2) COMMENT '消费最低值',
        PAY_AMNT_DELTA DECIMAL(17,2) COMMENT '消费值极差',
        ETL_TIME TIMESTAMP COMMENT '时间'
        ) partitioned by (STATIS_DATE string comment '数据日期' )
        stored as rcfile""")
// when the number of extremum at NanJing(025) less then 3 ,it's means table has been truncated and should be update.
    val querySql = "select * from SNGMSVC.T_MOB_CUMULATE_EXTREMUM where statis_date = '"+statis_date+"'"
    val dfExtrm = spark.sql(querySql)
    produceGivenDurationExtremum(statis_date,-7,spark)

  }
}
