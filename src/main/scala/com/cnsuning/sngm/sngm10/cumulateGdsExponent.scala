package com.cnsuning.sngm.sngm10

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

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
    val queryStrResMap = "select city_cd,str_type,str_cd,res_cd,distance from sospdm.sngm_store_res_map t  "//where city_cd = '025'
    val queryPayByStrGds = "select statis_date,city_code city_cd,res_cd,dept_cd,gds_cd,pay_amnt from sospdm.sngm_t_order_width_07_d where " +
      "statis_date <='"+statis_date+"' and statis_date >'"+DateUtils(statis_date,"yyyyMMdd",duration-30)+"' "//and city_code = '025'
    val queryTypeDeptMap = "select str_type,dept_cd from sospdm.sngm_type_dept_td a"

//    do lazy query and transform
    val dfStrDept = spark.sql(queryTypeDeptMap)
    dfStrDept.persist(StorageLevel.MEMORY_ONLY)

    val dfOriginalResMap = spark.sql(queryStrResMap)
      .repartition(col("city_cd"))
    val dfOriginalResPayBrand = spark.sql(queryPayByStrGds)
      .repartition(col("statis_date"),col("city_cd"),col("res_cd"),col("dept_cd"))
    val df1 = dfOriginalResMap.filter(col("distance")<=5).repartition(col("city_cd"))
    df1.persist(StorageLevel.MEMORY_ONLY)
    val df2 = dfOriginalResMap.filter(col("distance")<=1)

//    do summary by store res gds per day
    val dfPay = dfOriginalResPayBrand.groupBy("statis_date","city_cd","res_cd","dept_cd","gds_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))
    dfPay.persist(StorageLevel.MEMORY_ONLY)

    //    filter distance< 5km ,and summary pay amount on every day every store every brand
    val dfPay5km = df1.join(dfStrDept,Seq("str_type"),"left")
      .join(dfPay,Seq("city_cd","dept_cd","res_cd"),"inner")
      .groupBy("statis_date","city_cd","str_type","str_cd","gds_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))

    //    same approach to get 1km
    val dfPay1km = df2.join(dfStrDept,Seq("str_type"),"left")
      .join(dfPay,Seq("city_cd","dept_cd","res_cd"),"inner")
      .groupBy("statis_date","city_cd","str_type","str_cd","gds_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))

//    dfPay1km.createOrReplaceTempView("dfPay1kmPerGds"+duration.abs.toString)
//    dfPay5km.createOrReplaceTempView("dfPay5kmPerGds"+duration.abs.toString)
//  use spark core function to get summary
    val dfPayGdsDetail = dfPay5km.union(dfPay1km).repartition(col("city_cd"),col("str_type"),col("str_cd"),col("gds_cd"))
    val rankSpec = Window.partitionBy(col("city_cd"),col("str_type"),col("str_cd"),col("gds_cd"))
                        .orderBy(col("statis_date").asc).rowsBetween(duration+1,0)

    val dfQuantile1 = dfPayGdsDetail.withColumn("pay_amnt",sum("pay_amnt").over(rankSpec))
      .filter(col("statis_date") > DateUtils(statis_date,"yyyyMMdd",-30))
      .groupBy("city_cd","str_type","str_cd")
      .agg(max("pay_amnt").as("pay_amnt_max"),min("pay_amnt").as("pay_amnt_min"))

/*  abandoned step
    use window function to get summary pay amount of ${duration} days recently on every store's every Gds . then get the maximum and minimum.
    val dfQuantile = spark.sql(" select city_cd,str_type,str_cd, " +
      "max(pay_amnt) pay_amnt_max,min(pay_amnt) pay_amnt_min " +
      "from ( " +
              "select statis_date,city_cd,str_type,str_cd,pay_amnt " +
                "from ( " +
                           "select statis_date,city_cd,str_type,str_cd,gds_cd, " +
                                "sum(pay_amnt) over(partition by city_cd,str_type,str_cd,gds_cd order by statis_date asc rows  between "+(duration.abs-1)+" preceding and current row) pay_amnt " +
                            "from dfPay5kmPerGds"+duration.abs.toString +
                            " union all "+
                           "select statis_date,city_cd,str_type,str_cd,gds_cd, " +
                                 "sum(pay_amnt) over(partition by city_cd,str_type,str_cd,gds_cd order by statis_date asc rows  between "+(duration.abs-1)+" preceding and current row) pay_amnt " +
                            "from dfPay1kmPerGds"+duration.abs.toString +
                    " ) t where statis_date >'"+DateUtils(statis_date,"yyyyMMdd",-30)+"' " +
      ") t group by city_cd,str_type,str_cd ")
*/
    //  define etl_time
    val now = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val etl_time = dateFormat.format(now)

    val dfExtremum = dfQuantile1.withColumn("pay_amnt_min",when(col("pay_amnt_min") < 0 ,lit(0)).otherwise(col("pay_amnt_min")))
      .withColumn("pay_amnt_delta",col("pay_amnt_max") - col("pay_amnt_min"))
      .withColumn("etl_time",lit(etl_time))
      .withColumn("duration",lit(duration.abs))

    dfPay.unpersist()
    df1.unpersist()
    dfStrDept.unpersist()
    dfExtremum
  }

  /*
  *  this function used to trunsformed statis_date's ${duration} days' pay amount  to exponent at every store's gds
  *  within res's distance limited in 1/3/5km
  * */
  def produceCurrentDateGdsExponent(statis_date:String,duration:Int,spark:SparkSession):DataFrame={
    spark.sql("use sngmsvc")
    // define query sentence
    val queryPayByRes = "select city_code city_cd,res_cd,dept_cd,gds_cd,gds_nm,pay_amnt from sospdm.sngm_t_order_width_07_d t where " +
      "statis_date <= '"+ statis_date +"' " +
      "and statis_date > '"+DateUtils(statis_date,"yyyyMMdd",duration)+"' "/*and city_code = '025'*/ //sales detail of current date's ${duration} days past

    val queryStrResMap = "select city_cd,str_cd,res_cd,str_type,distance from sospdm.sngm_store_res_map t "//where city_cd='025'
    val queryStrDetail = "select str_cd,str_nm,str_type,city_nm from sospdm.t_sngm_init_str_detail " // where city_cd='025'
    val queryExtremum = "select city_cd,str_cd,cumulate_days,pay_amnt_max,pay_amnt_min,pay_amnt_delta " +
      "from sngmsvc.t_mob_cumulate_gds_extremum where statis_date='"+statis_date+"'  " //and city_cd ='025'
    val queryTypeDeptMap = "select str_type,dept_cd from sospdm.sngm_type_dept_td a"

    // do lazy query and transform
    val dfPayByResGds = spark.sql(queryPayByRes).repartition(col("city_cd"),col("dept_cd"),col("res_cd"),col("gds_cd"))
    dfPayByResGds.persist(StorageLevel.MEMORY_ONLY)

    val dfOriginalResMap = spark.sql(queryStrResMap).repartition(col("city_cd"))
    val dfStr = spark.sql(queryStrDetail)
    val dfExtremum = spark.sql(queryExtremum)
      .withColumnRenamed("cumulate_days","day")
      .filter(col("day") === duration.abs.toString)

    val dfStrDept = spark.sql(queryTypeDeptMap)
    dfStrDept.persist(StorageLevel.MEMORY_ONLY)

    val dfPayByStrGdsAgg = dfPayByResGds.groupBy("city_cd","dept_cd","res_cd","gds_cd")
                            .agg(sum("pay_amnt").as("pay_amnt"))
    dfPayByStrGdsAgg.persist(StorageLevel.MEMORY_ONLY)

    // get unique gds_cd mapping to gds_nm
    val dfGds = dfPayByResGds.select("gds_cd","gds_nm").distinct()
    val rankSpec = Window.partitionBy("gds_cd").orderBy(col("gds_nm").asc)
    val dfGdsUnique = dfGds.withColumn("row_number",row_number().over(rankSpec))
                      .filter(col("row_number") === 1 ).drop("row_number")
    dfGdsUnique.persist(StorageLevel.MEMORY_ONLY)
    dfGdsUnique.write.mode("overwrite").saveAsTable("sngmsvc.t_mob_cumulate_gds_detail_tmp")

    // get store-res mapping on distance
    val df5 = dfOriginalResMap.filter(col("distance") <= 5)
    df5.persist(StorageLevel.MEMORY_ONLY)
    val df3 = df5.filter(col("distance") <= 3)
    val df1 = df3.filter(col("distance") <= 1)

    // do join to limit pay amount in 5/3/1 km
    val dfPay5km = df5.join(dfStrDept,Seq("str_type"),"left")
      .join(dfPayByStrGdsAgg,Seq("city_cd","dept_cd","res_cd"),"inner")
      .groupBy("city_cd","str_cd","gds_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))
      .withColumn("distance",lit("5km"))
    dfPay5km.persist(StorageLevel.MEMORY_ONLY)
    dfPay5km.write.mode("overwrite").saveAsTable("sngmsvc.dfPay5km_tmp")

    val dfPay3km = df3.join(dfStrDept,Seq("str_type"),"left")
      .join(dfPayByStrGdsAgg,Seq("city_cd","dept_cd","res_cd"),"inner")
      .groupBy("city_cd","str_cd","gds_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))
      .withColumn("distance",lit("3km"))

    val dfPay1km = df1.join(dfStrDept,Seq("str_type"),"left")
      .join(dfPayByStrGdsAgg,Seq("city_cd","dept_cd","res_cd"),"inner")
      .groupBy("city_cd","str_cd","gds_cd")
      .agg(sum("pay_amnt").as("pay_amnt"))
      .withColumn("distance",lit("1km"))

    // union all distance DataFrame
    val dfPayUnion = dfPay5km.union(dfPay3km).union(dfPay1km)
      .join(dfStr,Seq("str_cd"),"left")
      .join(dfGdsUnique,Seq("gds_cd"),"left")

    //  define etl_time
    val now = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val etl_time = dateFormat.format(now)

    // mark store' gds pay amount to be a exponent .
    val dfExponent = dfPayUnion.join(dfExtremum,Seq("city_cd","str_cd"),"left")
      .withColumn("pay_expnt",(col("pay_amnt")-col("pay_amnt_min"))/col("pay_amnt_delta"))
      .withColumn("pay_expnt",when(col("pay_expnt")>1,lit(9999)).otherwise(when(col("pay_expnt")<0.001,lit(10)).otherwise(col("pay_expnt")*10000)))
      .withColumn("statis_date",lit(statis_date))
      .withColumn("etl_time",lit(etl_time))

    dfPayByResGds.unpersist()
    dfStrDept.unpersist()
    dfPayByStrGdsAgg.unpersist()
    dfGdsUnique.unpersist()
    df5.unpersist()

    dfExponent
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
      .set("spark.sql.auto.repartition","true")
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

//    get and save extremum
    produceGivenDurationGdsExtremum(statis_date,-7,spark)
      .union(produceGivenDurationGdsExtremum(statis_date,-15,spark))
      .union(produceGivenDurationGdsExtremum(statis_date,-30,spark))
      .createOrReplaceTempView("dfGdsExtremumCumulate")

    spark.sql("insert overwrite table sngmsvc.t_mob_cumulate_gds_extremum partition(statis_date='"+statis_date+"') " +
      "select city_cd,str_cd,duration,pay_amnt_max,pay_amnt_min,pay_amnt_delta,etl_time from dfGdsExtremumCumulate")

//    get and save exponent
    produceCurrentDateGdsExponent(statis_date,-15,spark)
      .union(produceCurrentDateGdsExponent(statis_date,-30,spark))
      .union(produceCurrentDateGdsExponent(statis_date,-7,spark))
        .createOrReplaceTempView("t_mob_cumulate_exponent_gds_view")


    spark.sql("insert overwrite table sngmsvc.t_mob_cumulate_exponent_gds_d partition(statis_date ='"+statis_date+"') " +
      " select city_cd,city_nm,str_type,str_cd,str_nm,distance,day,gds_cd,gds_nm,pay_expnt,etl_time from t_mob_cumulate_exponent_gds_view")

    spark.stop()
  }
}
