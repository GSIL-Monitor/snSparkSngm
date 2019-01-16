package com.cnsuning.sngm.scalaSrc
//http://www.cnblogs.com/MOBIN/p/5618747.html
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{lit, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  *
  */
object normalTest {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local") .setAppName("testGrouping")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val dataFrame:DataFrame = spark.createDataFrame(Seq(
      ("ALIPAY", "1001", "蚂蚁-支付宝-技术部"),
      ("ALIPAY", "1002", "阿里-阿里云-市场部"),
      ("ALIPAY", "1003", "阿里-天猫-无线事业部"),
      ("ALIPAY", "1004", "阿里-市场部"),
      ("TAOBAO", "1001", "蚂蚁-支付宝-技术部"),
      ("TAOBAO", "1004", "阿里-市场部"),
      ("TMALL", "1001", "蚂蚁-支付宝-技术部"),
      ("TMALL", "1003", "阿里-天猫-无线事业部"),
      ("CAINIAO", "1001", "蚂蚁-支付宝-技术部")
    )).toDF("pro_id","emp_id","dept")
    val ds = dataFrame.as[(String,String,String)]
    // 第一小题答案
    val ds1= ds.flatMap{
      case(pro_id,emp_id,dept) => {
        var tmp:String = ""
        for( curr <- dept.split("-") ) yield {
            tmp =  tmp.concat(curr).concat("-")
            (pro_id,emp_id,tmp.dropRight(1))
          }
      }
    }.toDF("pro_id","emp_id","dept")
      .as[(String,String,String)]
//      .distinct()
    val ds2 = ds1.groupBy("pro_id","dept").count()
        .withColumnRenamed("count","uv")
    ds2.show(100)

//    //第二小题答案
    val ds3 = ds2.filter(!ds2.col("dept").contains("-"))
    val ds4 = ds3.groupBy("pro_id").agg(lit("ALL").as("dept"),sum("uv").as("uv"))
    val ds5 = ds3.union(ds4)
    ds5.show()
    ds5.write.mode("append").saveAsTable("tmp_log_res_1")
  }
}
