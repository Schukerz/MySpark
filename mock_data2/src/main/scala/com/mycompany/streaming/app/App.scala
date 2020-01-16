package com.mycompany.streaming.app

import com.mycompany.streaming.bean.AdsInfo
import com.mycompany.streaming.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait App {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("App")
    val ssc = new StreamingContext(conf,Seconds(3))

    //2.获取Kafka的DStream
    val kafkaDStream: DStream[AdsInfo] = MyKafkaUtil.createKafkaDStream(ssc, "ads_log")
      .map(v => {
        //1579101508084,华北,北京,103,2
        val splits: Array[String] = v.split(",")
        AdsInfo(
          splits(0).toLong,
          splits(1),
          splits(2),
          splits(3),
          splits(4)
        )
      })

    //3.操作DStream
    doSomething(kafkaDStream)

    //4.开启ssc并阻塞等待
    ssc.start()
    ssc.awaitTermination()
  }
  def doSomething(stream: DStream[AdsInfo]):Unit

}
