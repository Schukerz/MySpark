package com.atguigu.mock.streaming.project.app

import com.atguigu.mock.streaming.project.bean.AdsInfo
import com.atguigu.mock.streaming.project.utils.MyKafkaUtil
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait App {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("kafka").setMaster("local[*]")
    val ssc:StreamingContext = new StreamingContext(conf,streaming.Seconds(3))
    ssc.checkpoint("./ck")
    //从kafka读数据
    val sourceDStream: DStream[AdsInfo] = MyKafkaUtil.getKafkaStream(ssc, "ads_log").map(s => {
      val splits: Array[String] = s.split(",")
      //1579078380980,华中,成都,100,4
      AdsInfo(
        splits(0).toLong,
        splits(1),
        splits(2),
        splits(3),
        splits(4)
      )
    })


    //2.操作DStream
    //TODO
    doSomething(sourceDStream)
    //3.启动ssc和阻止main方法退出
    ssc.start()
    ssc.awaitTermination()
  }
  def doSomething(sourceDStream:DStream[AdsInfo]):Unit
}
