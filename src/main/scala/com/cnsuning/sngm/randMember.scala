package com.cnsuning.sngm
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.reflect.ClassTag
/**
  *
  *
  */
object randMember {
  def main(args: Array[String]): Unit = {
    val memGrp = Array("潘先召","任强","周莉","丁涛","任政","宋凯 ","孙林","张旭 ","张雨生","鲍孙俏","邓振国","黄杰","吕小柱","倪俊","孙俊","王本松","王云宝","谢舒怀","左健")
//    takeSample(memGrp,3,5).foreach(println)
    val pos = ArrayBuffer[Int]()
    var i = 0
    while(i < 3){
     val r = (new util.Random).nextInt(memGrp.length)
     if (pos.contains(r))
       i = i-1
      else
       pos += r
      i = i+1
    }
    pos.foreach(x => println(memGrp(x)))
//    println(memGrp.length)
  }

//  def takeSample[T:ClassTag](a:Array[T],n:Int,seed:Long) = {
////    val rnd = new Random(seed)
//    val rnd = new Random()
//    Array.fill(n)(a(rnd.nextInt(a.size)))
//  }
}
