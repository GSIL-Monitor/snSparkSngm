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
    /*接收并处理时间参数*/
    val statis_date = args(0)
//    val  = args(1)
    val logger = LoggerFactory.getLogger(activityManageListDaily.getClass)

    val sc = new SparkConf().setAppName("actMngLstDlyExector").setMaster("local")
//    val sparkSession = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()
    val sparkContext = new SparkContext(sc)
    val rdd = sparkContext.parallelize(args.toSeq)
    logger.info("==============Args_output===============" + ":{}",rdd.collect())
//    logger.info("==============argsLength===============" + ":{}",args.length)
//    logger.info("==============argsforeach===============" + ":{}",v1)
    logger.info("==============${statisdate}===============" + ":{}",args.foreach(x => x))

//    args.foreach(x => logger.info(x))

  }
}
