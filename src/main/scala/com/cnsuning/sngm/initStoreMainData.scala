package com.cnsuning.sngm

/**
  *
  *
  */
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.lit

object initStoreMainData {
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
    val data4 = data3.withColumn("clrCityName",when({{data3.col(cityCol).like("%市") || data3.col(cityCol).like("州")} && length(data3.col(cityCol))>2},data3.col("clrCityName")).otherwise(data3.col(cityCol)))
    data4.withColumn(strCol,regexp_replace(data4.col(strCol),data4.col("clrCityName"),data4.col("zero")))
      .drop("zero").drop("clrCityName")
      .withColumnRenamed("startPos","data_source")
    //    DF
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("initStoreMainData")
      //      .setMaster("local")
      .set("spark.sql.hive.metastorePartitionPruning", "false")

    //    define value for program
    val querySqlStr="select plant_cd,plant_nm,cmpy_cd,area_cd,city_cd,city_nm,street_cd,idry_site_tp_cd,sub_plant_tp_cd,plant_tp_cd,plant_stat_cd from bi_sor.tsor_org_plant_td t "
    val querySqlCity = "select admin_city_cd,city_cd from bi_sor.tsor_org_city_conv_rel_td t  "
    val querySqlStrType = "select idry_site_tp_cd,sub_plant_tp_cd,str_type from sospdm.store_type_logic t"
    val session = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()

    //    get store information with admin city_cd
    val str = session.sql(querySqlStr)
    val strDFin = str.withColumn("admin_city_cd",str("city_cd").cast("Int"))
      .drop("city_cd")
      .where("plant_tp_cd = 2 and plant_stat_cd = 1")
      .drop("plant_tp_cd").drop("plant_stat_cd")
      .filter(row => {val plt_nm = row.getAs[String]("plant_nm")
        val st_cd = row.getAs[String]("street_cd")
        plt_nm != "待定" && plt_nm != "待用" && st_cd != "待定" && st_cd != "待用" && !plt_nm.contains("测试")
      })

    //  change store name to be a real store name when it contain its' city name within a UDF Function
    val strDF = clearStoreName(strDFin,"plant_nm","city_nm")

    //    get city information to replace the admin city_cd
    val cityDF = session.sql(querySqlCity)
      .filter(row => row.getAs[String]("city_cd").length == 3 /*&& row.getAs[String]("city_cd") not like */)
      .filter(!col("city_cd").like("HK%"))
      .distinct()

    //    get store type information to be joined
    val strType = session.sql(querySqlStrType).distinct()

    //    inner join the store information to replace a admin_city_cd became a city_cd: 1000017 -> 025
    val DF = strDF
      .join(cityDF,"admin_city_cd")
      .withColumnRenamed("plant_cd","str_cd")
      .withColumnRenamed("plant_nm","str_nm")
      .withColumnRenamed("street_cd","addr")
      .withColumnRenamed("cmpy_cd","org_cd")
      .withColumn("longitude",lit(0))
      .withColumn("latitude",lit(0))
      .withColumn("data_source",lit(0))
      .withColumn("district_nm",lit(""))
      .drop("admin_city_cd")
    //      .drop("org_cd")

    val rsltDF = DF.join(strType,Seq("idry_site_tp_cd","sub_plant_tp_cd"),"inner")
      .drop("idry_site_tp_cd").drop("sub_plant_tp_cd")

    rsltDF.write.mode("overwrite").saveAsTable("sospdm.t_sngm_init_str_original") // 此句会Drop原先的表，然后按照DF的内容自己建立一个。




    //    LOGGER.info("==================STR_count=================" + ": {}", strDF.count())

    session.stop()
  }
}
