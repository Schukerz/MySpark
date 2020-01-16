package com.atguigu.mock.streaming.project.app

import com.atguigu.mock.streaming.project.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

object AreaAdsClickTop3App extends App {
  override def doSomething(stream: DStream[AdsInfo]): Unit ={
    //1.每天没地区没广告的点击量
    val dayAreaAdsAndCountDStream: DStream[((String, String, String), Int)] = stream.map(info => (info.dayString, info.area, info.adsId) -> 1)
      .updateStateByKey((seq:Seq[Int], opt:Option[Int]) => Some(opt.getOrElse(0) + seq.sum))
    //2.每天每地区分组
    val dayAreaAndAdsCountGrouped: DStream[((String, String), Iterable[(String, Int)])] = dayAreaAdsAndCountDStream.map {
      case ((day, area, ads), count) => ((day, area), (ads, count))
    }.groupByKey()
    val resultStream: DStream[((String, String), List[(String, Int)])] = dayAreaAndAdsCountGrouped.map {
      case ((day, area), adsCountIt) =>
        (day, area) -> adsCountIt.toList.sortBy(-_._2).take(3)
    }
    resultStream.print(1000)
  }
}
