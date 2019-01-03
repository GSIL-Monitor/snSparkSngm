package com.cnsuning.sngm.scalaSrc

object caseTest {
  def judgeGrade(grade:String,name:String)=
    grade match {
      case "A" => println("Excellent!")
      case "B" => println("Good!")
      case "C" => println("just so so")
      case grd if name == "fibonacci" => println("you are goods boy" + " -- " + grd)
        // 如有常量需要使用 则大写常量 否则会被认为是变量并覆盖，若常量小写，则使用``反引号包裹
      case _ => println("you need study hard")
    }

  def card[T](content:T){
    content match {
      case cont:String => println("content is String type and is " + cont)
      case cont:Int => println("content is Int type and is " + cont)
    }
  }

  def main(args: Array[String]): Unit = {
//    judgeGrade("S","fibona1cci")
    card("str")
    card(123)
  }
}
