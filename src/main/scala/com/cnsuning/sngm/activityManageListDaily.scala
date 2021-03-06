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
      " from sospdm.t_sngm_activity_base_info t where activity_id='201810131658074042'"
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
      val querySqlOrderWidth = "select statis_date,chnl_cd,dept_cd,brand_cd,city_code,str_cd,res_cd,member_id,pay_amnt from sospdm.sngm_t_order_width_07_d a " +
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
        .withColumn("statis_date_w",from_unixtime(dfAct1.col("start_date_burst") - (((dfAct1.col("start_date_burst") - 1522512000) / 86400) % 7 - 7) * 86400,"yyyyMMdd")) //得到当周最后一天（周日）
        .withColumn("statis_date_m",from_unixtime(dfAct1.col("start_date_burst"),"yyyyMM"))

      val dfCt = spark.sql(querySqlCity)
      val dfActJoin = spark.sql(querySqlActJoin)
      val dfActRef = spark.sql(querySqlActRef)

      val dfActDtl1 =
          dfActDtl.join(dfActRef,"activity_id")
              .join(dfCt,Seq("area_cd"),"left")
              .join(dfActJoin,Seq("activity_id","city_cd"),"left")

      import spark.implicits._
      val dsActDtl = dfActDtl1.as[(String,String,String,
        Long,Long,Long,
        String,String,String,
        Long,Long,
        String,String,String,String,
        Int,
        String,String,String,String)]

      val dfActDtl2 = dsActDtl.flatMap{
        case (activity_id,city_cd,area_cd,
        start_date_outside,start_date_burst,end_date_burst,
        activity_nm,activity_type,str_type,
        start_date_comparison,end_date_comparison,
        activity_state,statis_date_d,statis_date_w,statis_date_m,
        marketing_type,
        marketing_cd,city_nm,str_cd,res_cd) => {for(statis_date <- start_date_burst to end_date_burst by 86400) yield dataFormat.format(statis_date * 1000)}.map((
          activity_id,city_cd,area_cd,
          start_date_outside,_,
          activity_nm,activity_type,str_type,
          activity_state,statis_date_d,statis_date_w,statis_date_m,
          marketing_type,
          marketing_cd,city_nm,str_cd,res_cd,"burst"
        ))
      }.toDF("activity_id","city_cd","area_cd",
        "start_date_outside","statis_date",
        "activity_nm","activity_type","str_type",
        "activity_state","statis_date_d","statis_date_w","statis_date_m",
        "marketing_type",
        "marketing_cd","city_nm","str_cd","res_cd","periodtype")

    val dfActDtl3 = dsActDtl.flatMap{
      case (activity_id,city_cd,area_cd,
      start_date_outside,start_date_burst,end_date_burst,
      activity_nm,activity_type,str_type,
      start_date_comparison,end_date_comparison,
      activity_state,statis_date_d,statis_date_w,statis_date_m,
      marketing_type,
      marketing_cd,city_nm,str_cd,res_cd) => {for(statis_date <- start_date_comparison to end_date_comparison by 86400) yield dataFormat.format(statis_date * 1000)}.map((
        activity_id,city_cd,area_cd,
        start_date_outside,_,
        activity_nm,activity_type,str_type,
        activity_state,statis_date_d,statis_date_w,statis_date_m,
        marketing_type,
        marketing_cd,city_nm,str_cd,res_cd,"comparison"
      ))
    }.toDF("activity_id","city_cd","area_cd",
      "start_date_outside","statis_date",
      "activity_nm","activity_type","str_type",
      "activity_state","statis_date_d","statis_date_w","statis_date_m",
      "marketing_type",
      "marketing_cd","city_nm","str_cd","res_cd","periodtype")

    val dfActDtl4 = dfActDtl2.union(dfActDtl3)

    val dfActDtlDept = dfActDtl4.filter(col("marketing_type") === "1").withColumnRenamed("marketing_cd","dept_cd")
    val dfActDtlBrand = dfActDtl4.filter(col("marketing_type") === "2").withColumnRenamed("marketing_cd","brand_cd")
    val dfOrdWidth = spark.sql(querySqlOrderWidth)
    val dfActSaleDtl = dfActDtlDept.join(dfOrdWidth,Seq("statis_date","dept_cd","res_cd","str_cd"))
      .withColumn("chnl_cd",when(col("chnl_cd").cast("Int") < 50 ,"2").otherwise("1"))
      .withColumnRenamed("dept_cd","goods_cd")



    dfActDtlDept.write.mode("overwrite").saveAsTable("sospdm.t_sngm_act1_test")
    dfOrdWidth.write.mode("overwrite").saveAsTable("sospdm.t_sngm_act1_test0")
    dfActSaleDtl.write.mode("overwrite").saveAsTable("sospdm.t_sngm_act1_test1")

      dfAct1.unpersist()
//    logger.info("==============${statisdate}===============" + ":{}",querySqlOrderWidth)

//    close spark session  select a.* from sospdm.t_sngm_act1_test1 a where city_cd='635' and goods_cd='00001'and str_cd='9671'
    spark.stop()
  }
}
