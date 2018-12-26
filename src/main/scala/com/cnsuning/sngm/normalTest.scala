package com.cnsuning.sngm

/**
  *
  *
  */
object normalTest {
  def asList[T](pSrc:Array[T]): List[T] ={
    if(pSrc == null || pSrc.isEmpty)
      List[T]()
    else
      pSrc.toList
  }

//  def asListUseBound[T <: ]

  def main(args: Array[String]): Unit = {

    val friends = Array("阿猫","阿狗","阿猪")
    val friendList = asList(friends)
    println(friendList.isInstanceOf[List[String]])

    println("---------------- 1 ----------- test from class type bound")

//    val s = [(String,String,Int)]
//    println(s.getClass)
  }
}
