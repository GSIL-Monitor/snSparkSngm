package com.cnsuning.sngm.sngm10

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * name:cumulateGdsExponent
  * description: NoDetail
  * running mode:Appended by daily frequency
  * target:SNGMSVC.
  * createdate:2019/1/25
  * author:18040267
  * email:ericpan24@gmail.com
  * copyRight:Suning
  * modifyby:
  * modifydate:
  */
object cumulateGdsExponent {
  def produceGivenDurationGdsExtremum(statis_date:String,duration:Int,spark:SparkSession):DataFrame={
    spark.sql("use sngmsvc")
    val queryStrResMap = "select city_cd,str_type,str_cd,res_cd,distance from sospdm.sngm_store_res_map t where city_cd = '025' "
    val queryPayByStrGds = "select statis_date,city_code city_cd,res_cd,str_cd,gds_cd,pay_amnt from sospdm.sngm_t_order_width_07_d where " +
      "statis_date <='"+statis_date+"' and statis_date >'"+DateUtils(statis_date,"yyyyMMdd",duration-30)+"' and city_code = '025'"

//    do lazy query and transform
val dfOriginalResMap = spark.sql(queryStrResMap)
    val dfOriginalStrPayBrand = spark.sql(queryPayByStrGds)
    val df1 = dfOriginalResMap.filter(col("distance")<=5)
    val df2 = dfOriginalResMap.filter(col("distance")<=1)

//    do summary by store res gds per day
    val dfPay = dfOriginalStrPayBrand.groupBy("statis_date","city_cd","res_cd","str_cd","gds_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))

    //    filter distance< 5km ,and summary pay amount on every day every store every brand
    val dfPay5km = df1.join(dfPay,Seq("city_cd","str_cd","res_cd"),"inner")
      .groupBy("statis_date","city_cd","str_type","str_cd","gds_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))

    //    same approach to get 1km
    val dfPay1km = df2.join(dfPay,Seq("city_cd","str_cd","res_cd"),"inner")
      .groupBy("statis_date","city_cd","str_type","str_cd","gds_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))

    dfPay1km.createOrReplaceTempView("dfPay1kmPerGds"+duration.abs.toString)
    dfPay5km.createOrReplaceTempView("dfPay5kmPerGds"+duration.abs.toString)
//    use window function to get summary pay amount of ${duration} days recently on every store's every Gds . then get the maximum and minimum.
    val dfQuantile = spark.sql(" select city_cd,str_type,str_cd, " +
      "max(pay_amnt) pay_amnt_max,min(pay_amnt) pay_amnt_min " +
      "from ( " +
              "select statis_date,city_cd,str_type,str_cd,pay_amnt " +
                "from ( " +
                           "select statis_date,city_cd,str_type,str_cd,gds_cd, " +
                                "sum(pay_amnt) over(partition by city_cd,str_type,str_cd,brand_cd order by statis_date asc rows  between "+(duration.abs-1)+" preceding and current row) pay_amnt " +
                            "from dfPay5kmPerGds"+duration.abs.toString +
                            " union all "+
                           "select statis_date,city_cd,str_type,str_cd,gds_cd, " +
                                 "sum(pay_amnt) over(partition by city_cd,str_type,str_cd,brand_cd order by statis_date asc rows  between "+(duration.abs-1)+" preceding and current row) pay_amnt " +
                            "from dfPay1kmPerGds"+duration.abs.toString +
                    " ) t where statis_date >'"+DateUtils(statis_date,"yyyyMMdd",-30)+"' " +
      ") t group by city_cd,str_type,str_cd ")
    //  define etl_time
    val now = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val etl_time = dateFormat.format(now)

    val dfExtremum = dfQuantile.withColumn("pay_amnt_min",when(col("pay_amnt_min") < 0 ,lit(0)).otherwise(col("pay_amnt_min")))
      .withColumn("pay_amnt_delta",col("pay_amnt_max") - col("pay_amnt_min"))
      .withColumn("etl_time",lit(etl_time))
      .withColumn("duration",lit(duration.abs))
    dfExtremum
  }

  /*
  * main function
  * */
  def main(args: Array[String]): Unit = {
    //    receive parameter from suning IDE dispatcher
    val statis_date = args(0)
    val lstMon = args(1)
    //    define spark session
    val sc = new SparkConf().setAppName("cumulateBrandExponent")
      .set("spark.sql.hive.metastorePartitionPruning", "false")
    val spark = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()
    //    create temp table
    spark.sql("use sngmsvc")
    spark.sql("""CREATE TABLE IF NOT EXISTS SNGMSVC.T_MOB_CUMULATE_GDS_EXTREMUM(
                 CITY_CD STRING COMMENT '城市编码',
                 STR_CD STRING COMMENT '门店编码',
                 CUMULATE_DAYS STRING COMMENT '累计天数',
                 PAY_AMNT_MAX DECIMAL(17,2) COMMENT '消费最高值',
                 PAY_AMNT_MIN DECIMAL(17,2) COMMENT '消费最低值',
                 PAY_AMNT_DELTA DECIMAL(17,2) COMMENT '消费值极差',
                 ETL_TIME TIMESTAMP COMMENT '时间'
    ) PARTITIONED BY (STATIS_DATE STRING COMMENT '数据日期')
    STORED AS RCFILE""")

    produceGivenDurationGdsExtremum(statis_date,-7,spark)
      .union(produceGivenDurationGdsExtremum(statis_date,-15,spark))
      .union(produceGivenDurationGdsExtremum(statis_date,-30,spark))
      .createOrReplaceTempView("dfGdsExtremumCumulate")
    spark.sql("insert overwrite table sngmsvc.t_mob_cumulate_brand_extremum partition(statis_date='"+statis_date+"') " +
      "select city_cd,str_cd,duration,pay_amnt_max,pay_amnt_min,pay_amnt_delta,etl_time from dfGdsExtremumCumulate")
  }
}
