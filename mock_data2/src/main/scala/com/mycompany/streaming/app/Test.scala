package com.mycompany.streaming.app

import java.text.SimpleDateFormat
import java.util.Date

object Test {
  def main(args: Array[String]): Unit = {
//    val ts: TimeStamp = new TimeStamp("1579102367720")
    val date = new Date(1579102624256L)
    val str: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)
    println(str)
  }

}
