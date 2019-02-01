package com.cnsuning.sngm.sngm10

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * name:dateUtils
  * description: dateUtils has designed to simpled date process
  * running mode:
  * target:
  * createdate:2019/1/25
  * author:18040267
  * email:ericpan24@gmail.com
  * copyRight:Suning
  * modifyby:
  * modifydate:
  */
class DateUtils(val date:String,val pattern:String,val duration:Int){
  val dateFormat = new SimpleDateFormat(pattern)
  val cal = Calendar.getInstance

  def process:String = {
    val d = dateFormat.parse(date)
    cal.setTime(d)
    cal.add(Calendar.DAY_OF_MONTH,duration)
    dateFormat.format(cal.getTime)
  }
}
object DateUtils{
  def apply(date:String,pattern:String,duration:Int):String = {
    val d = new DateUtils(date,pattern,duration)
    d.process
  }
}
