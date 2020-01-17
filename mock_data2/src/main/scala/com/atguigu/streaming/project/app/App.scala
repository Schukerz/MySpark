package com.atguigu.streaming.project.app

import com.atguigu.streaming.project.bean.AdsInfo
import com.atguigu.streaming.project.utils.KafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait App {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("App").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("./ck")
    //2.从kafka中读取数据

   val kafkaDStream: DStream[String] = KafkaUtil.getKafkaConsumer(ssc,"ads_log")

    val sourceDStream: DStream[AdsInfo] = kafkaDStream.map(info => {
      //1579227708164,华北,北京,105,5
      val splits: Array[String] = info.split(",")
      AdsInfo(
        splits(0).toLong,
        splits(1),
        splits(2),
        splits(3),
        splits(4)
      )
    })

    //3.对RDD进行操作
    doSomething(sourceDStream)

    //4.启动ssc并阻塞当前线程
    ssc.start()
    ssc.awaitTermination()
  }
  def doSomething(sourceDStream:DStream[AdsInfo]):Unit
}
