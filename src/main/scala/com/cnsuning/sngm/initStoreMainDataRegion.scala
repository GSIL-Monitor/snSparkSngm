package com.cnsuning.sngm

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.util.matching.Regex
/**
  *
  *
  */
object initStoreMainDataRegion {
  val logger = LoggerFactory.getLogger(initStoreMainDataRegion.getClass)

  def main(args: Array[String]): Unit = {
    // init spark configuration
    val sc = new SparkConf().setAppName("initStoreMainDataRegion")
      .set("spark.sql.hive.metastorePartitionPruning", "false")
    val session = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()

    // define data source from hive sql
    val querySqlStr = "select str_cd,str_nm,city_cd,city_nm,addr,longitude,latitude,org_cd,area_cd,district_nm,str_type  from sospdm.t_sngm_store_detail t"
    val querySqlRegion = "select area_code,area_name,city_code from sospdm.t_area_detail t" // t4
    val querySqlCmpy = "select cmpy_nm,area_nm,str_cd,open_date from bi_sor.tsor_org_store_td t" //t2

    // get data frame of  data
    val dfStr = session.sql(querySqlStr)
    val dfRegion = session.sql(querySqlRegion)
    val dfCmpy = session.sql(querySqlCmpy)

    // clean the column str_cd
    val dfStr1 = dfStr.withColumn("str_cd",regexp_replace(dfStr.col("str_cd"),"\t|\n|\r|\\s+",""))
    //   rename for the column
    val dfRegion1 =
      dfRegion.withColumnRenamed("area_name","district_nm")
        .withColumnRenamed("area_code","region_cd")
        .withColumnRenamed("city_code","city_cd")
        .distinct()
    val dfRegion2 = dfRegion1.withColumn("district_nm",regexp_replace(dfRegion1.col("district_nm"),"区|市|县",""))
    // rename for the column
    val dfCmpy1 = dfCmpy.withColumnRenamed("cmpy_nm","org_nm")
      .withColumnRenamed("open_date","built_date")

    //define etl_time
    val now = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val etl_time = dateFormat.format(now)

    // use left join to combine  three tables and get distinct
    val df = dfStr1.join(dfCmpy1,Seq("str_cd"),"left")
      .join(dfRegion2,Seq("district_nm","city_cd"),"left")
      .distinct()
        .withColumn("etl_time",lit(etl_time))


//    logger.info("====================================================:{}",df.show())
    // split join result ,using column region_nm whether contains district_nm
//    val df1 = df.filter(row => row.getAs[String]("region_nm").contains( row.getAs[String]("district_nm")))
//    val df2 = df.filter(row => !row.getAs[String]("region_nm").contains( row.getAs[String]("district_nm")))

    // store date overwrite in to hive data warehouse，cover destination table DDl struct
    df.write.mode("overwrite").saveAsTable("sospdm.t_sngm_init_str_detail")

//    session.sql("insert overwrite table sosp_ssa.tssa_sosp_t_store_detail " +
//      "select str_cd,str_nm,city_cd,city_nm,addr,longitude,latitude,org_cd,org_nm,area_cd,area_nm,etl_time,district_nm,region_cd,str_type,built_date" +
//      "from sospdm.t_sngm_init_str_detail t")

    session.stop()
  }
}
