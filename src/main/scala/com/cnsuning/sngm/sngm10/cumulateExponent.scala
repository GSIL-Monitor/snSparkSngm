package com.cnsuning.sngm.sngm10

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

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
  def produceGivenDurationExtremum(statis_date:String,duration:Int,spark:SparkSession):DataFrame = {
    spark.sql("use sngmsvc")
//    define query sentence,statis_date in ( 30+duration days past ,current date] !!!
    val queryStrResMap = "select city_cd,str_type,str_cd,res_cd,distance from sospdm.sngm_store_res_map t "/*where city_cd = '025'*/
    val queryPayByRes = "select statis_date,city_code city_cd,res_cd,dept_cd,pay_amnt from sospdm.sngm_t_order_width_07_d where " +
      "statis_date <='"+statis_date+"' and statis_date >'"+DateUtils(statis_date,"yyyyMMdd",duration-30)+"' "/*and city_code = '025'*/
    val queryTypeDeptMap = "select str_type,dept_cd from sospdm.sngm_type_dept_td a"

//    do lazy query and transform
    val dfOriginalResMap = spark.sql(queryStrResMap)
    val dfOriginalStrPay = spark.sql(queryPayByRes)
    val dfStrDept = spark.sql(queryTypeDeptMap)
    dfStrDept.persist(StorageLevel.MEMORY_ONLY)
    val df1 = dfOriginalResMap.filter(col("distance")<=5)
    df1.persist(StorageLevel.MEMORY_ONLY)
    val df2 = df1.filter(col("distance")<=1)

//    do summary by store res per day
    val dfPay = dfOriginalStrPay.groupBy("statis_date","city_cd","res_cd","dept_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))

//    get the dept_cd of store in sale,then join res pay amount on dept within 5km at every day.
    val dfPay5kmPerStr = df1.join(dfStrDept,Seq("str_type"),"left")
      .join(dfPay,Seq("city_cd","dept_cd","res_cd"),"inner")
      .groupBy("statis_date","city_cd","str_type","str_cd").agg(sum("pay_amnt").as("pay_amnt"))

//    same approach to get 1 km
    val dfPay1kmPerStr = df2.join(dfStrDept,Seq("str_type"),"left")
      .join(dfPay,Seq("city_cd","dept_cd","res_cd"),"inner")
      .groupBy("statis_date","city_cd","str_type","str_cd").agg(sum("pay_amnt").as("pay_amnt"))

//  put pay amount of every store every day on 1&5km at res to hive temproray table ,then use hive's advance function percentile and over window.
    dfPay5kmPerStr.createOrReplaceTempView("dfPay5kmPerStr"+duration.abs.toString)
    dfPay1kmPerStr.createOrReplaceTempView("dfPay1kmPerStr"+duration.abs.toString)

//  use window function to get summary pay amount of ${duration} days recently on every store . then get the 25% & 75% quantile.
    val dfQuantile = spark.sql(" select city_cd,str_type," +
      "percentile_approx(pay_amnt,0.75) pay_amnt_75,percentile_approx(pay_amnt,0.25) pay_amnt_25,max(pay_amnt) pay_amnt_max,min(pay_amnt) pay_amnt_min " +
      "from ( " +
            "select statis_date,city_cd,str_type,str_cd,pay_amnt " +
            "from ( " +
                    "select statis_date,city_cd,str_type,str_cd, " +
                            "sum(pay_amnt) over(partition by city_cd,str_type,str_cd order by statis_date asc rows  between "+(duration.abs-1)+" preceding and current row) pay_amnt " +
                    "from dfPay5kmPerStr"+duration.abs.toString +
                    " union all "+
                    "select statis_date,city_cd,str_type,str_cd, " +
                            "sum(pay_amnt) over(partition by city_cd,str_type,str_cd order by statis_date asc rows  between "+(duration.abs-1)+" preceding and current row) pay_amnt " +
                    "from dfPay1kmPerStr"+duration.abs.toString +
                  " ) t where statis_date >'"+DateUtils(statis_date,"yyyyMMdd",-30)+"' " +
      ") t group by city_cd,str_type ")
//    dfQuantile.persist(StorageLevel.MEMORY_ONLY)
//    dfQuantile.write.mode("overwrite").saveAsTable("sngmsvc.t_mob_cumulate_extremum_tmp1"+duration.abs.toString)

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
        .withColumn("duration",lit(duration.abs)) // 注明 数据的累计天数,即跨度
      .drop("pay_amnt_75").drop("pay_amnt_25")
    dfStrDept.unpersist()
    df1.unpersist()

    dfExtremum //  return a DataFrame
  }

  /*
  *  this function used to trunsformed statis_date's ${duration} days' pay amount  to exponent at every city's store
  *  within res's distance limited in 1/3/5km
  * */
  def produceCurrentDateExponent(statis_date:String,lstMon:String,duration:Int,spark:SparkSession):DataFrame={
    spark.sql("use sngmsvc")
//    define query sentence
    val queryPayByRes = "select city_code city_cd,res_cd,dept_cd,pay_amnt,0 pay_amnt_comp from sospdm.sngm_t_order_width_07_d t where " +
      "statis_date <= '"+ statis_date +"' " +
      "and statis_date > '"+DateUtils(statis_date,"yyyyMMdd",duration)+"' "/*and city_code = '025'*/ //sales detail of current date's ${duration} days past

    val queryPayByResComp = "select city_code city_cd,res_cd,dept_cd,0 pay_amnt,pay_amnt pay_amnt_comp from sospdm.sngm_t_order_width_07_d t where " +
      "statis_date <= '"+ lstMon +"' " +
      "and statis_date > '"+DateUtils(lstMon,"yyyyMMdd",duration)+"' " /*and city_code = '025'*/ //sales detail of compare date's ${duration} days past

    val queryStrResMap = "select city_cd,str_cd,res_cd,str_type,distance from sospdm.sngm_store_res_map t "/*where city_cd='025'*/
    val queryStrDetail = "select str_cd,str_type,str_nm,city_nm from sospdm.t_sngm_init_str_detail "/*where city_cd='025'*/
    val queryExtremum = "select city_cd,str_type,cumulate_days,pay_amnt_max,pay_amnt_min,pay_amnt_delta " +
      "from sngmsvc.t_mob_cumulate_extremum where statis_date='"+statis_date+"' "/*  and city_cd ='025'*/
    val queryTypeDeptMap = "select str_type,dept_cd from sospdm.sngm_type_dept_td a"

//    do lazy query and transform
    val dfPayByRes = spark.sql(queryPayByRes).union(spark.sql(queryPayByResComp))
      .groupBy("city_cd","res_cd","dept_cd")
      .agg(sum("pay_amnt").as("pay_amnt"),sum("pay_amnt_comp").as("pay_amnt_comp"))
    dfPayByRes.persist(StorageLevel.MEMORY_ONLY)

    val dfStrDept = spark.sql(queryTypeDeptMap)
    dfStrDept.persist(StorageLevel.MEMORY_ONLY)

    val dfStr = spark.sql(queryStrDetail)
    val dfExtremum = spark.sql(queryExtremum)
      .withColumnRenamed("cumulate_days","day")
      .filter(col("day") === duration.abs.toString)

    val dfOriginalResMap = spark.sql(queryStrResMap)
    val df5 = dfOriginalResMap.filter(col("distance") <= 5)
    df5.persist(StorageLevel.MEMORY_ONLY)
    val df3 = df5.filter(col("distance") <= 3)
    val df1 = df3.filter(col("distance") <= 1)

//    do join to limit pay amount in 5/3/1 km
    val dfPay5km = df5.join(dfStrDept,Seq("str_type"),"left")
      .join(dfPayByRes,Seq("city_cd","dept_cd","res_cd"),"inner")
      .groupBy("city_cd","str_cd")
      .agg(sum("pay_amnt").as("pay_amnt"),sum("pay_amnt_comp").as("pay_amnt_comp"))
      .withColumn("distance",lit("5km"))

    val dfPay3km = df3.join(dfStrDept,Seq("str_type"),"left")
      .join(dfPayByRes,Seq("city_cd","dept_cd","res_cd"),"inner")
      .groupBy("city_cd","str_cd")
      .agg(sum("pay_amnt").as("pay_amnt"),sum("pay_amnt_comp").as("pay_amnt_comp"))
      .withColumn("distance",lit("3km"))

    val dfPay1km = df1.join(dfStrDept,Seq("str_type"),"left")
      .join(dfPayByRes,Seq("city_cd","dept_cd","res_cd"),"inner")
      .groupBy("city_cd","str_cd")
      .agg(sum("pay_amnt").as("pay_amnt"),sum("pay_amnt_comp").as("pay_amnt_comp"))
      .withColumn("distance",lit("1km"))

    val dfPayUnion = dfPay5km.union(dfPay3km).union(dfPay1km)
      .join(dfStr,Seq("str_cd"),"left")

    //  define etl_time
    val now = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val etl_time = dateFormat.format(now)

    //   make pay amount and compare pay amount to be a exponent.
    val dfExponent = dfPayUnion.join(dfExtremum,Seq("city_cd","str_type"),"left")
      .withColumn("pay_expnt",(col("pay_amnt")-col("pay_amnt_min"))/col("pay_amnt_delta"))
      .withColumn("pay_expnt",when(col("pay_expnt")>1,lit(9999)).otherwise(when(col("pay_expnt")<0.001,lit(10)).otherwise(col("pay_expnt")*10000)))
      .withColumn("pay_amnt_incrs_rate",col("pay_amnt")/col("pay_amnt_comp")-1)
      .withColumn("statis_date",lit(statis_date))
      .withColumn("etl_time",lit(etl_time))
    dfPayByRes.unpersist()
    dfStrDept.unpersist()
    df5.unpersist()

    dfExponent
  }

  /*
  *  main function
  * */
  def main(args: Array[String]): Unit = {
//    receive parameter from suning IDE dispatcher
    val statis_date = args(0)
    val lstMon = args(1)
//    define spark session
    val sc = new SparkConf().setAppName("cumulateExponent")
      .set("spark.sql.hive.metastorePartitionPruning", "false")
    val spark = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()
//    create temp table
    spark.sql("use sngmsvc")
    spark.sql(
      """CREATE TABLE IF NOT EXISTS SNGMSVC.T_MOB_CUMULATE_EXTREMUM(
        CITY_CD STRING COMMENT '城市编码',
        STR_TYPE STRING COMMENT '门店业态编码',
        CUMULATE_DAYS STRING COMMENT '累计天数',
        PAY_AMNT_MAX DECIMAL(17,2) COMMENT '消费最高值',
        PAY_AMNT_MIN DECIMAL(17,2) COMMENT '消费最低值',
        PAY_AMNT_DELTA DECIMAL(17,2) COMMENT '消费值极差',
        ETL_TIME TIMESTAMP COMMENT '时间'
        ) partitioned by (STATIS_DATE string comment '数据日期' )
        stored as rcfile""")
//  获取各门店30天内分别在1&5km距离商圈上的（7/15/30天）累计销售明细，并按箱线图四分位数逻辑去除离群值，得到各业态的极值和极差。
    produceGivenDurationExtremum(statis_date,-7,spark)
      .union(produceGivenDurationExtremum(statis_date,-15,spark))
      .union(produceGivenDurationExtremum(statis_date,-30,spark))
      .createOrReplaceTempView("dfExtremumCumulate")
//  save the result of union to partitioned table .
    spark.sql("insert overwrite table sngmsvc.t_mob_cumulate_extremum partition(statis_date='"+statis_date+"') " +
      "select city_cd,str_type,duration,pay_amnt_max,pay_amnt_min,pay_amnt_delta,etl_time from dfExtremumCumulate")

//  do produce statis_date's exponent

    val dfResult = produceCurrentDateExponent(statis_date,lstMon,-7,spark)
        .union(produceCurrentDateExponent(statis_date,lstMon,-15,spark))
        .union(produceCurrentDateExponent(statis_date,lstMon,-30,spark))

    dfResult.write.mode("overwrite").saveAsTable("sngmsvc.t_mob_cumulate_exponent_d_tmp")

    spark.sql("insert overwrite table sngmsvc.t_mob_cumulate_exponent_d partition(statis_date='"+statis_date+"') " +
      "select city_cd,city_nm,str_type,str_cd,str_nm,distance,day,pay_expnt,pay_amnt_incrs_rate,etl_time from sngmsvc.t_mob_cumulate_exponent_d_tmp ")
    spark.stop()
  }
}
