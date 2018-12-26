package com.cnsuning.sngm

import org.apache.spark.sql.{Dataset,SparkSession}
import org.apache.spark.sql.functions.sum


class GroupingSets[T]{
  private[this] var dimArr:Array[String] = _
  private[this] var mesSeq:Seq[(String,String,String)] = _
  private var rsltDS:Dataset[T] = _
  private var orignlDS:Dataset[T] = _
  private var tmpDS:Dataset[T] = _

  def this(ds:Dataset[T],gp:Array[String],ag:Seq[(String,String,String)]){
    this()
    orignlDS = ds
    dimArr = gp
    mesSeq = ag
  }
  def this(ds:Dataset[T],gp:Array[String],ag:Seq[(String,String,String)],dsTmp:Dataset[T]){
    this(ds,gp,ag)
    tmpDS = dsTmp
  }

  def groupby() ={
     val ip = GroupingSets.sparkStatic.implicits
    import ip._
    val dstmp = this.orignlDS.groupBy(this.dimArr.head,this.dimArr.tail:_*)
        .agg(sum(this.mesSeq.head._3).as(this.mesSeq.head._1),this.mesSeq.tail.map(x => sum(x._3).as(x._3)):_*)
        .as[(String,String,String,BigInt,BigInt,BigInt,BigInt,BigInt)]
//    tmpDS = dstmp
  }

  def show= {
    println(dimArr)
    println(mesSeq)
//    GroupingSets.sparkStatic
    if(this.tmpDS == null)
      println("ds2 is null")
    else
      println("ds 2 is not null")
  }
}

object GroupingSets{
  private var sparkStatic:SparkSession = _
  def apply[T](spark:SparkSession,ds:Dataset[T],gp:Array[String],ag:Seq[(String,String,String)])={
    sparkStatic = spark
//    import spark.implicits._
    println("apply1")
    new GroupingSets(ds,gp,ag)
  }

  def apply[T](spark:SparkSession,ds:Dataset[T],gp:Array[String],ag:Seq[(String,String,String)],dsTmp:Dataset[T]) ={
    sparkStatic = spark
    println("apply2")
    new GroupingSets(ds,gp,ag,dsTmp)
  }
}
