package com.mycompany.streaming.app
import com.mycompany.streaming.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

object AreaAdsClickTop3App extends App {
  override def doSomething(stream: DStream[AdsInfo]): Unit = {
    stream.print(1000)
  }
}
