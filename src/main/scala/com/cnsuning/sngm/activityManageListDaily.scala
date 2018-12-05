package com.cnsuning.sngm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**Program used to deal with Activity Management List module(AML), on daily frequency
  *
  *
  */


object activityManageListDaily {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(activityManageListDaily.getClass)

    val sc = new SparkConf().setAppName("actMngLstDlyExector").setMaster("local")
//    val sparkSession = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()
    val sparkContext = new SparkContext(sc)
    val rdd = sparkContext.parallelize(args.toSeq)
    logger.info("==============Args_output===============" + ":{}",rdd.collect())
  }
}
