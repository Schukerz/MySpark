package com.atguigu.mock.streaming.project.app
import com.atguigu.mock.streaming.project.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

class LastHourAdsClickApp extends App {
  override def doSomething(sourceDStream: DStream[AdsInfo]): Unit = {}
}
