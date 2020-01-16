package com.atguigu.mock.streaming.project.app

import com.atguigu.mock.streaming.project.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

//最近 1 小时广告点击量实时统计
/*
1.划分窗口
2.跳转数据类型((ads,hm),1)
3.统计
4.写入到redis
 */
class LastHourAdsClickApp extends App {
  override def doSomething(sourceDStream: DStream[AdsInfo]): Unit = {


  }
}

