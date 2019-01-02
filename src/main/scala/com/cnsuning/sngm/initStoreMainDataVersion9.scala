package com.cnsuning.sngm

/**
  *
  *
  */
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object initStoreMainDataVersion9 {
  val LOGGER = LoggerFactory.getLogger(initStoreMainData.getClass)

  def clearStoreName(DF:DataFrame,strCol:String,cityCol:String):DataFrame = {
    val data1 = DF.withColumn("zero",lit(""))
      .withColumn("startPos",lit(0))
    //1 将门店名包含的城市名删掉
    val data2 = data1.withColumn(strCol,regexp_replace(data1.col(strCol),data1.col(cityCol),data1.col("zero")))
    //2 去除最后一个字的城市名称
    /* val data3 = data2.select(
        col(strCol)
       ,col(cityCol)
       ,col("zero")
       ,col(cityCol).substr(data2.col("startPos"),length(data2.col(cityCol))-1).as("clrCityName")
     )*/
    val data3 = data2.withColumn("clrCityName",data2.col(cityCol).substr(data2.col("startPos"),length(data2.col(cityCol))-1))
    val data4 = data3.withColumn("clrCityName",when({{data3.col(cityCol).like("%市") || data3.col(cityCol).like("%州")} && length(data3.col(cityCol))>2},data3.col("clrCityName")).otherwise(data3.col(cityCol)))
    data4.withColumn(strCol,regexp_replace(data4.col(strCol),data4.col("clrCityName"),data4.col("zero")))
      .drop("zero").drop("clrCityName")
      .withColumnRenamed("startPos","data_source")
    //    DF
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("initStoreMainData")
      .set("spark.sql.hive.metastorePartitionPruning", "false")

    //    define value for program
    val querySqlStr="select str_cd,str_nm,cmpy_cd,cmpy_nm,area_cd,city_cd,city_nm,str_addr,str_busi_fmt_cd,str_status,site_tp_cd,vrtl_flg,str_prop_cd from brock_dim.t_spl_str_tree_ed a"
//    val querySqlCity = "select admin_city_cd,city_cd from bi_sor.tsor_org_city_conv_rel_td t  "
    val querySqlStrType = "select idry_site_tp_cd,sub_plant_tp_cd,str_type from sospdm.store_type_logic t"
    val querySqlStrPos = "select bd_longitude,bd_latitude,store_code from pos_sor.tsor_nsfbus_md_store_operate_all"
    val session = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()

    //    get store information with admin city_cd
    val str = session.sql(querySqlStr)
    val strDFin = str
      .where("str_status = 1 and site_tp_cd = 2 and vrtl_flg = 0 ")
      .drop("str_status").drop("site_tp_cd").drop("vrtl_flg")
      .filter(row => {val str_nm = row.getAs[String]("str_nm")
        val str_addr = row.getAs[String]("str_addr")
        str_nm != "待定" && str_nm != "待用" && str_addr != "待定" && str_addr != "待用" &&
          !str_nm.contains("测试") && !str_nm.contains("验证") &&
          !str_nm.contains("续签") && !str_nm.contains("费用") &&
          !str_nm.contains("虚拟")
      })
      .filter(!col("city_cd").like("HK%"))
//      .filter(!col("str_addr")==="-")
//      .filter(!col("str_nm").like("%虚拟%"))
      .withColumnRenamed("cmpy_cd","org_cd")
      .withColumnRenamed("str_addr","addr")
      .withColumnRenamed("str_busi_fmt_cd","idry_site_tp_cd")
      .withColumnRenamed("str_prop_cd","sub_plant_tp_cd")
//      .withColumn("longitude",lit(0))
//      .withColumn("latitude",lit(0))
      .withColumn("district_nm",lit(""))

    //  change store name to be a real store name when it contain its' city name within a UDF Function
    val strDF = clearStoreName(strDFin,"str_nm","city_nm")

    //    get city information to replace the admin city_cd
//    val cityDF = session.sql(querySqlCity)
//      .filter(row => row.getAs[String]("city_cd").length == 3 /*&& row.getAs[String]("city_cd") not like */)
//      .filter(!col("city_cd").like("HK%"))
//      .distinct()

    //    get store type information to be joined
    val strType = session.sql(querySqlStrType).distinct()

    //    get hive level store location position from main data
    val strPos = session.sql(querySqlStrPos).distinct()
      .withColumnRenamed("bd_longitude","longitude")
      .withColumnRenamed("bd_latitude","latitude")
      .withColumnRenamed("store_code","str_cd")

    //    inner join the store information to replace a admin_city_cd became a city_cd: 1000017 -> 025
//    val DF = strDF
//      .join(cityDF,"admin_city_cd")
//      .withColumnRenamed("plant_cd","str_cd")
//      .withColumnRenamed("plant_nm","str_nm")
//      .withColumnRenamed("street_cd","addr")
//      .withColumnRenamed("cmpy_cd","org_cd")
//      .withColumn("longitude",lit(0))
//      .withColumn("latitude",lit(0))
//      .withColumn("data_source",lit(0))
//      .withColumn("district_nm",lit(""))
//      .drop("admin_city_cd")
    //      .drop("org_cd")
val now = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val etl_time = dateFormat.format(now)

    val rsltDF = strDF.join(strType,Seq("idry_site_tp_cd","sub_plant_tp_cd"),"inner")
      .drop("idry_site_tp_cd").drop("sub_plant_tp_cd")
        .join(strPos,Seq("str_cd"),"left")
        .withColumn("etl_time",lit(etl_time))

    rsltDF.write.mode("overwrite").saveAsTable("sospdm.t_sngm_init_str_original") // 此句会Drop原先的表，然后按照DF的内容自己建立一个。




    //    LOGGER.info("==================STR_count=================" + ": {}", strDF.count())

    session.stop()
  }
}
