package com.cnsuning.sngm.sngm10

import java.text.SimpleDateFormat
import java.util.Date

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
  def produceLastMonthExtremum(statis_date:String,lstMon:String,spark:SparkSession):Unit = {
    spark.sql("use sngmsvc")

//    define query sentence
    val queryStrResMap = "select city_cd,str_type,str_cd,res_cd,distance from sospdm.sngm_store_res_map t where city_cd='025'"
    val queryPayByStr = "select statis_date,city_code city_cd,res_cd,str_cd,pay_amnt from sospdm.sngm_t_order_width_07_d where " +
                        "statis_date <='" + statis_date + "' and statis_date > '" + lstMon +"' and city_code='025'"

//    do lazy query and transform
    val dfOriginalResMap = spark.sql(queryStrResMap)
    val dfOriginalStrPay = spark.sql(queryPayByStr)
    val df1 = dfOriginalResMap.filter(dfOriginalResMap.col("distance") <= 5)
    val df2 = df1.filter(df1.col("distance") <= 1)

//    do summary by store res per day
    val dfPay1 = dfOriginalStrPay.groupBy("statis_date","city_cd","res_cd","str_cd").agg(sum("pay_amnt").as("pay_amnt"))

//    get summary pay amount of 5 km store-res mapping ,then do summary to get store pay amount at every day.
    val dfPay5kmPerStr =
      df1.join(dfPay1,Seq("city_cd","str_cd","res_cd"),"inner")
        .groupBy("statis_date","city_cd","str_type","str_cd").agg(sum("pay_amnt").as("pay_amnt"))

//    same approach as maximum to get 1 km summary
    val dfPay1kmPerStr =
      df2.join(dfPay1,Seq("city_cd","str_cd","res_cd"),"inner")
        .groupBy("statis_date","city_cd","str_type","str_cd").agg(sum("pay_amnt").as("pay_amnt"))

//  combine 5km and 1km store  pay amount in one DataFrame by everyday. Put DF as a temproray view in hive
    dfPay5kmPerStr.union(dfPay1kmPerStr).createOrReplaceTempView("dfPayPerStr")

//    dfTmp.persist(StorageLevel.MEMORY_ONLY_2)
//    dfTmp.createOrReplaceTempView("dfPayPerStr")
//    dfTmp.write.mode("overwrite").saveAsTable("sngmsvc.t_mob_fullyear_extremum_tmp1")

//  get pay amount quantile of 0.25 & 0.75
    val  dfQuantile = spark.sql("select city_cd,str_type," +
      "percentile_approx(pay_amnt,0.75) pay_amnt_75,percentile_approx(pay_amnt,0.25) pay_amnt_25,min(pay_amnt) pay_amnt_min " +
      "from dfPayPerStr a group by city_cd,str_type ")
//  define etl_time
    val now = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val etl_time = dateFormat.format(now)

//  redefined the boundary of pay amount to remove outliers
    val dfExtremum =
      dfQuantile.withColumn("pay_amnt_max",col("pay_amnt_75") + col("pay_amnt_75")*1.5 -col("pay_amnt_25")*1.5)
        .withColumn("pay_amnt_min",when(col("pay_amnt_min") < 0 ,lit(0)).otherwise(col("pay_amnt_min")))
        .withColumn("pay_amnt_delta",col("pay_amnt_max") - col("pay_amnt_min"))
        .withColumn("etl_time",lit(etl_time))

//    dfExtremum.persist(StorageLevel.MEMORY_ONLY_2)
//    dfExtremum.write.mode("overwrite").saveAsTable("sngmsvc.t_mob_fullyear_extremum_tmp2")
//    dfExtremum
//      .drop("pay_amnt_25").drop("pay_amnt_75")

    dfExtremum.createOrReplaceTempView("dfExtremum")
    spark.sql("insert overwrite table SNGMSVC.T_MOB_TREND_EXTREMUM partition(statis_date='"+statis_date+"') " +
      " select city_cd,str_type,pay_amnt_max,pay_amnt_min,pay_amnt_delta,etl_time from dfExtremum")
  }

  /**
    * this function used to transformed statis_date's  pay amount to a Exponent at every city's store type,distance of store with res limited in 5/3/1 kilometer.
    * */
  def produceCurrentDateExponent(statis_date:String,lstMon:String,spark:SparkSession): Unit ={
    spark.sql("use sngmsvc")

//    define query sentence
    val queryPayByStr = "select city_code city_cd,res_cd,str_cd,pay_amnt from sospdm.sngm_t_order_width_07_d t where " +
      "statis_date = '"+ statis_date +"' and city_code = '025'" //sales detail of current date
    val queryPayByStrComp = "select city_code city_cd,res_cd,str_cd,pay_amnt from sospdm.sngm_t_order_width_07_d t where " +
      "statis_date = '"+ lstMon +"' and city_code = '025'" //sales detail of one month ago

    val queryStrResMap = "select city_cd,str_cd,res_cd,distance from sospdm.sngm_store_res_map t where city_cd='025'"
    val queryStrDetail = "select str_cd,str_nm,str_type,city_nm from sospdm.t_sngm_init_str_detail where city_cd='025'"
    val queryExtremum = "select city_cd,str_type,pay_amnt_max,pay_amnt_min,pay_amnt_delta from sngmsvc.t_mob_trend_extremum where statis_date='"+statis_date+"' and city_cd ='025'"
//    do lazy query and transform
    val dfOriginalResMap = spark.sql(queryStrResMap)
    val dfOriginalStrPay = spark.sql(queryPayByStr)
    val dfOriginalStrPayComp = spark.sql(queryPayByStrComp)
    val dfStr = spark.sql(queryStrDetail)
    val dfExtremum = spark.sql(queryExtremum)

    val df5 = dfOriginalResMap.filter(col("distance") <= 5)
    val df3 = df5.filter(col("distance") <= 3)
    val df1 = df3.filter(col("distance") <= 1)
//    do summary by store res of current date
    val dfPayCurr = dfOriginalStrPay.groupBy("city_cd","res_cd","str_cd")
      .agg(sum("pay_amnt").as("pay_amnt"),lit(0).as("pay_amnt_comp"))
//    do summary by store res of the day last month ago
    val dfPayComp =dfOriginalStrPayComp.groupBy("city_cd","res_cd","str_cd")
      .agg(lit(0).as("pay_amnt"),sum("pay_amnt").as("pay_amnt_comp"))
//    combine current with last month pay amount
    val dfPay = dfPayCurr.union(dfPayComp).groupBy("city_cd","res_cd","str_cd")
      .agg(sum("pay_amnt").as("pay_amnt"),sum("pay_amnt_comp").as("pay_amnt_comp"))

//    do join to limit pay amount in 5/3/1 km
    val dfPay5km = df5.join(dfPay,Seq("city_cd","str_cd","res_cd"),"inner")
      .groupBy("city_cd","str_cd")
      .agg(sum("pay_amnt").as("pay_amnt"),sum("pay_amnt_comp").as("pay_amnt_comp"))
      .withColumn("distance",lit("5km"))
    val dfPay3km = df3.join(dfPay,Seq("city_cd","str_cd","res_cd"),"inner")
      .groupBy("city_cd","str_cd")
      .agg(sum("pay_amnt").as("pay_amnt"),sum("pay_amnt_comp").as("pay_amnt_comp"))
      .withColumn("distance",lit("3km"))
    val dfPay1km = df1.join(dfPay,Seq("city_cd","str_cd","res_cd"),"inner")
      .groupBy("city_cd","str_cd")
      .agg(sum("pay_amnt").as("pay_amnt"),sum("pay_amnt_comp").as("pay_amnt_comp"))
      .withColumn("distance",lit("1km"))

    val dfPayUnion = dfPay5km.union(dfPay3km).union(dfPay1km)
      .join(dfStr,Seq("str_cd"),"left")
//    dfPayUnion.persist(StorageLevel.MEMORY_ONLY_2)
//    dfPayUnion.write.mode("overwrite").saveAsTable("sngmsvc.t_mob_fullyear_extremum_tmp3")

//  define etl_time
    val now = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val etl_time = dateFormat.format(now)

//   make pay amount and compare pay amount to be a exponent.
    val dfExponent = dfPayUnion.join(dfExtremum,Seq("city_cd","str_type"),"left")
      .withColumn("pay_expnt",(col("pay_amnt")-col("pay_amnt_min"))/col("pay_amnt_delta"))
      .withColumn("pay_expnt_comp",(col("pay_amnt_comp")-col("pay_amnt_min"))/col("pay_amnt_delta"))
      .withColumn("pay_expnt",when(col("pay_expnt")>1,lit(9999)).otherwise(when(col("pay_expnt")<0.001,lit(10)).otherwise(col("pay_expnt")*10000)))
      .withColumn("pay_expnt_comp",when(col("pay_expnt_comp")>1,lit(9999)).otherwise(when(col("pay_expnt_comp")<0.001,lit(10)).otherwise(col("pay_expnt_comp")*10000)))
      .withColumn("statis_date",lit(statis_date))
      .withColumn("etl_time",lit(etl_time))

    dfExponent.createOrReplaceTempView("dfExponent")
    spark.sql("insert overwrite table SNGMSVC.T_MOB_TREND_EXPONENT_D partition(statis_date='"+statis_date+"') " +
      " select city_cd,city_nm,str_type,str_cd,str_nm,distance,pay_amnt,pay_expnt,pay_expnt_comp,etl_time from dfExponent")
//    dfExponent.write.mode("overwrite").saveAsTable("sngmsvc.T_MOB_TREND_EXPONENT_D")
  }

  // main function
  def main(args: Array[String]): Unit = {
  // receive parameter from suning IDE dispatcher
  val statis_date = args(0) // current date yyyyMMdd
  val lstMon = args(1) // one month before current date yyyyMMdd
  val lstYr = args(2) //last year
  val curYr = args(3) //current year
  // define spark session
  val sc = new SparkConf().setAppName("exponentTrend")//.setMaster("local")
      .set("spark.sql.hive.metastorePartitionPruning", "false")
  val spark = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()

  // create temp table
  spark.sql("use sngmsvc")
  spark.sql("""CREATE TABLE IF NOT EXISTS SNGMSVC.T_MOB_TREND_EXTREMUM (
              CITY_CD STRING COMMENT '城市编码',
              STR_TYPE STRING COMMENT '门店业态编码',
              PAY_AMNT_MAX DECIMAL(17,2) COMMENT '消费最高值',
              PAY_AMNT_MIN DECIMAL(17,2) COMMENT '消费最低值',
              PAY_AMNT_DELTA DECIMAL(17,2) COMMENT '消费值极差',
              ETL_TIME TIMESTAMP COMMENT '时间'
              ) partitioned by (STATIS_DATE string comment '数据日期' )
              stored as rcfile""")

//   获取各门店30天内分别在1&5km距离商圈上的每日销售明细，并按箱线图四分位数逻辑去除离群值，得到各业态的极值和极差。
    produceLastMonthExtremum(statis_date,lstMon,spark)


// do produce statis_date's exponent
  produceCurrentDateExponent(statis_date,lstMon,spark)
  spark.stop()
  }
}
