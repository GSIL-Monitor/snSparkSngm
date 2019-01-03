package com.cnsuning.sngm.SparkStreaming

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.ServerSocket
import scala.io.Source
/**
  *
  *
  */
object ServerSocketSource {
  def main(args: Array[String]): Unit = {

    for(i <- 0 until 3 ){
      val SS = new ServerSocket(7777)
      val CS = SS.accept()
      val outputStream = new ObjectOutputStream(CS.getOutputStream)
      val filePointer = Source.fromFile("C:/Users/18040267/Desktop/1.txt")
      for( line <- filePointer.getLines() ){
        Thread.sleep(100)
        println(line)

        outputStream.writeObject(line)
        outputStream.flush()
      }

      println("=================================  No."+ i + " times file scan!  ================================")
    }
  }
}
