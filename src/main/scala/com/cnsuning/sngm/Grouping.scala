package com.cnsuning.sngm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
//import scala.reflect.runtime.{universe => ru}


class Grouping[T](ds:Dataset[T],gp:Array[String],ag:Seq[(String,String,String)]) {
  private[this] val dataSet:Dataset[T] = ds
  private[this] var dataSet1:Dataset[T] = ds
  private[this] val gpArr = gp
  private[this] val agSeq = ag
  println("Grouping Class first construct success")

  def this(ds:Dataset[T],gp:Array[String],ag:Seq[(String,String,String)],ds1:Dataset[T]){
    this(ds,gp,ag)
    this.dataSet1 = ds1
  }

  /*重写groupby方法实现接受一个字符串数组并执行dataSet的分组
  * 要实现这个做法，你需要在数组参数后添加一个冒号和一个 _* 符号
  * 个标注告诉编译器把 arr 的每个元素当作参数，而不是当作单一的参数传给 方法 。
  * 因此当形参为String*时，不能直接把类型为Array[String]的实参直接传入，需要通过:_*进行转换。
  * */
  def groupby(gpArr:Array[String] = this.gpArr,agSeq:Seq[(String,String,String)] = this.agSeq) ={
    val spark = Grouping.sparkStatic
    import spark.implicits._

    val dsgp = this.dataSet.groupBy(gpArr.head,gpArr.tail:_*) //当接收的形参为可变长参数时
//      .agg(sum("pay_amnt"))
        .agg(sum(agSeq.head._3).as(agSeq.head._1),agSeq.tail.map(x => sum(x._3).as(x._1)):_*)
        .as[(String,String,String,BigInt,BigInt,BigInt,BigInt,BigInt)]
    println("result class is " + ds.getClass)
    Grouping.apply(spark,this.dataSet,this.gpArr,this.agSeq)
  }

  def set(subGp:Array[String]) ={
    if(!subGp.map(gpArr.contains(_)).reduce(_&&_))//   gpArr.cont

//      gpArr.contains(subGp)
      println("===================================")

//      Grouping(this.dataSet,this.gpArr,this.agSeq)
    else{
//      val dsset = this.dataSet.union()
//      this.groupby(subGp)
      println("===================================")
    }
//    Grouping(spark,dsset,this.gpArr,this.agSeq)
  }

  def show= dataSet.show
}
/*
*
* 创建伴生对象 实现new功能
* */
object Grouping{
  private var dataFrameStatic:Dataset[Row] = _
  private var sparkStatic:SparkSession = _

  def apply[T](spark:SparkSession,df:Dataset[Row],gp:Array[String],ag:Seq[(String,String,String)],ds1:Dataset[T])= {
    sparkStatic = spark
    dataFrameStatic = df
//    new Grouping[T](df:Dataset[T],gp,ag,ds1)
  }

  def apply[T](spark:SparkSession,df:Dataset[T],gp:Array[String],ag:Seq[(String,String,String)])= {
    sparkStatic = spark
    new Grouping[T](df:Dataset[T],gp,ag)
  }
}
