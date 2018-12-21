package com.cnsuning.sngm

import org.apache.spark.sql.Dataset


class Grouping[T](ds:Dataset[T]) {
  private[this] var dataSet:Dataset[T] = ds

//  def set(str:String) ={
//    println(str)
//    dataFrame = dataFrame.groupby()
//      .union(dataFrame)
//    dataFrame
//  }
}

object Grouping{
  def apply(ds:Dataset[T])= {
//    new Grouping[T](ds:Dataset[T])
    print(ds)
  }
}
