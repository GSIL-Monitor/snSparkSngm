package com.cnsuning.sngm

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.matching.Regex
/**
  *
  *
  */
object initStoreMainDataRegion {

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
    // rename for the column
    val dfRegion1 =
      dfRegion.withColumnRenamed("area_name","district_nm")
        .withColumnRenamed("area_code","region_cd")
        .withColumnRenamed("city_code","city_cd")
        .distinct()
    val dfRegion2 = dfRegion1.withColumn("district_nm",regexp_replace(dfRegion1.col("district_nm"),"区|市|县",""))

    val dfCmpy1 = dfCmpy.withColumnRenamed("cmpy_nm","org_nm")

    // use left join to combine  three tables and get distinct
    val df = dfStr1.join(dfCmpy,dfStr1("str_cd") === dfCmpy("str_cd"),"left")
      .join(dfRegion2,Seq("district_nm","city_cd"),"left")
      .distinct()

    // split join result ,using column region_nm whether contains district_nm
//    val df1 = df.filter(row => row.getAs[String]("region_nm").contains( row.getAs[String]("district_nm")))
//    val df2 = df.filter(row => !row.getAs[String]("region_nm").contains( row.getAs[String]("district_nm")))

    // store date overwrite in to hive data warehouse，cover destination table DDl struct
    df.write.mode("overwrite").saveAsTable("sospdm.t_sngm_init_str_detail")


  }
}
