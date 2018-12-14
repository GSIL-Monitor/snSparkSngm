package com.cnsuning.sngm

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
  * Program used to deal with Activity Management List module(AML), on daily frequency
  *
  *
  */


object activityManageListDaily {
  def main(args: Array[String]): Unit = {

//  accept date parameter
    val executeDate = args(0)


//    define log out object
    val logger = LoggerFactory.getLogger(activityManageListDaily.getClass)

//    define spark session
    val sc = new SparkConf().setAppName("actMngLstDlyExector")//.setMaster("local")
        .set("spark.sql.hive.metastorePartitionPruning", "false")
    val spark = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()

//    define activity data source
    val querySqlActBaseInfo = "select " + "start_date_outside,start_date_burst,end_date_burst," +
      "activity_id,activity_nm,'2' activity_type,'3' str_type,area_cd,start_date_comparison,end_date_comparison" +
      " from sospdm.t_sngm_activity_base_info t "
//      "where a.act_state ='0' "

//    define city detail data source
    val querySqlCity = "select city_cd,city_nm,area_cd from sospdm.t_city_detail t"

//    define activity join info source
    val querySqlActJoin = "select str_cd,res_cd,activity_id,city_cd from sospdm.t_sngm_activity_join_info t"

//    define activity base information ref data source
    val querySqlActRef = "select activity_id,marketing_type,marketing_cd from sospdm.t_sngm_activity_base_info_ref t"

//    define data format
    val dataFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

//    1.define activity base info data frame ,and get maximum&minimum burst date and even maximum&minimum comparison data

    val dfAct = spark.sql(querySqlActBaseInfo)
    val dfAct1 =
      dfAct.withColumn("start_date_outside",unix_timestamp(dfAct.col("start_date_outside"),"yyyyMMdd"))
        .withColumn("start_date_burst",unix_timestamp(dfAct.col("start_date_burst"),"yyyyMMdd"))
        .withColumn("end_date_burst",unix_timestamp(dfAct.col("end_date_burst"),"yyyyMMdd"))
        .withColumn("start_date_comparison",unix_timestamp(dfAct.col("start_date_comparison"),"yyyyMMdd"))
        .withColumn("end_date_comparison",unix_timestamp(dfAct.col("end_date_comparison"),"yyyyMMdd"))
    dfAct1.persist(StorageLevel.MEMORY_ONLY_2) //cache data in memory for second use

//    2. define the order data source range from minDate to maxDate
    val dfTime = dfAct1.select(col("start_date_burst"),col("end_date_burst"),col("start_date_comparison"),col("end_date_comparison"))
    val seqMax:Seq[Any] = dfTime.groupBy().max().head.toSeq
    val seqMin:Seq[Any] = dfTime.groupBy().min().head.toSeq
    var maxDate:Long = if (seqMax.head.isInstanceOf[Long]) seqMax.head.asInstanceOf[Long] else 0
    var minDate:Long = if (seqMin.head.isInstanceOf[Long]) seqMin.head.asInstanceOf[Long] else 2144231661 //时间戳最大值 1544630400 -> 20181213
    for(i <- seqMax) if(i.isInstanceOf[Long]) maxDate = if(maxDate < i.asInstanceOf[Long]) i.asInstanceOf[Long] else maxDate
    for(i <- seqMin) if(i.isInstanceOf[Long]) minDate = if(minDate > i.asInstanceOf[Long]) i.asInstanceOf[Long] else minDate
    val maxDateCondition = dataFormat.format(maxDate * 1000) // spark unix time stamp only to seconds ,different with scala
    val minDateCondition = dataFormat.format(minDate * 1000)
    val querySqlOrderWidth = "select * from sospdm.sngm_t_order_width_07_d a " +
      "where statis_date >='" + minDateCondition + "' and statis_date <='" + maxDateCondition + "'"


//    3. get every activity detail information
    val executeDateStamp:Long = dataFormat.parse(executeDate).getTime / 1000 + 86400 // cast running date to a stamp Long type and plus one day for compare with the activity date
    val dfActDtl = dfAct1.withColumn("activity_state",
        when(dfAct1.col("start_date_outside") > executeDateStamp,"1")
          .when(dfAct1.col("start_date_outside") < executeDateStamp && dfAct1("start_date_burst") > executeDateStamp,"2")
          .when(dfAct1.col("start_date_burst") <= executeDateStamp && dfAct1("end_date_burst") >= executeDateStamp,"3")
          .when(dfAct1.col("end_date_burst") < executeDateStamp,"4")
          .otherwise("99"))
        .withColumn("end_date_comparison",when(dfAct1.col("start_date_burst") <= executeDateStamp && dfAct1.col("end_date_burst") >= executeDateStamp,dfAct1.col("start_date_comparison") - dfAct1.col("start_date_burst") + executeDateStamp)
            .otherwise(dfAct1.col("end_date_comparison") + 86400))
        .withColumn("statis_date_d",from_unixtime(dfAct1.col("start_date_burst"),"yyyyMMdd"))
        .withColumn("statis_date_w",((dfAct1.col("start_date_burst")-1522512000) / 86400) % 7)
        .withColumn("statis_date_m",from_unixtime(dfAct1.col("start_date_burst"),"yyyyMM"))

//    dfAct1.unpersist()
      dfActDtl.write.mode("overwrite").saveAsTable("sospdm.t_sngm_act1_test")


//    logger.info("==============${statisdate}===============" + ":{}",querySqlOrderWidth)
//    close spark session
    spark.stop()
  }
}
