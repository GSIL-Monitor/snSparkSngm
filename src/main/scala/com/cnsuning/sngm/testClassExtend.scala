package com.cnsuning.sngm

object testClassExtend {
  def main(args: Array[String]): Unit = {
    class Person{
      val name = "fibonacci"
      var age = 20

    }
    class Student(val score:String="S") extends Person {
      override val name: String = "student fibonacci"


      def prtSco=println(score)
    }
    val p1 = new Person
    val s1 = new Student
    println(p1.name)
    s1.prtSco
    val p2:Person = new Student("A")

//    println(p2.name)
    val s2:Student = if(p2.isInstanceOf[Student]) p2.asInstanceOf[Student] else null
//
    s2.prtSco

  }
}
