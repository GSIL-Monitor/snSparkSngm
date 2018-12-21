package com.cnsuning.sngm

import org.apache.spark.sql.Dataset


class Grouping[T](ds:Dataset[T]) {
  private[this] var dataSet:Dataset[T] = ds
  println("Grouping Class construct success")

  def set(str:String) ={
    println(str)
    dataSet.union(dataSet)
  }
}

object Grouping{
  def apply[T](ds:Dataset[T])= {
//    println()
    new Grouping[T](ds:Dataset[T])
  }
}
