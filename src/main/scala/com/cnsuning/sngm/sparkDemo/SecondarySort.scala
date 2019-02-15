package com.cnsuning.sngm.sparkDemo

import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * name:SecondSort
  * description: Demo for second sort ,function example
  * running mode:Appended by daily frequency
  * target:SNGMSVC.
  * createdate:2019/2/12
  * author:18040267
  * email:ericpan24@gmail.com
  * copyRight:Suning
  * modifyby:
  * modifydate:
  */
object SecondarySort {
  def main(args: Array[String]): Unit = {
    val spark :SparkSession = InitSpark("SecondarySort","local")
//    use SparkSession.sparkContext replace sparkContext
//    val rdd1 = spark.sparkContext.parallelize(Seq(111,222,333))
//    val rdd2 = rdd1.map(x => (x,x*2))
//    init original RDD
    val rdd = spark.sparkContext.textFile("./src/main/scala/com/cnsuning/sngm/sparkDemo/SecondarySortData.txt")
//    val pairRdd = rdd.map(x => (x.split("\t")(0),(x.split("\t")(1),x.split("\t")(2))))
    val pairRdd = rdd.map(_.split("\t")).map(x => (x(0),Tuple2(x(1).toInt,x(2).toInt)))
    val groups = pairRdd.groupByKey()
    groups.foreach(println)
  }
}

class SecondarySort {

}
