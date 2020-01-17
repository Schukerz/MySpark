package com.atguigu.streaming.project.bean

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

case class AdsInfo(
                  ts:Long,
                  area:String,
                  city:String,
                  userId:String,
                  adsId:String,
                  var timestamp:Timestamp = null,
                  var dayString:String = null,
                  var hmString:String = null

                  ){
  timestamp=new Timestamp(ts)
  private val date = new Date(ts)
  dayString= new SimpleDateFormat("yyyy-MM-dd").format(date)
  hmString = new SimpleDateFormat("HH:mm").format(date)
}
