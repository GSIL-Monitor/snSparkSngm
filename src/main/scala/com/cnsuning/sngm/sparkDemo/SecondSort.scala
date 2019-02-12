package com.cnsuning.sngm.sparkDemo

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
object SecondSort {
  def main(args: Array[String]): Unit = {
    val spark :SparkSession = InitSpark("SecondSort","local")
//    use SparkSession.sparkContext replace sparkContext
//    val rdd1 = spark.sparkContext.parallelize(Seq(111,222,333))
//    rdd1.map(_*2).foreach(println)

  }
}

class SecondSort {

}
