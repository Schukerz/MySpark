package com.atguigu.night

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("window2").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("./ck2")
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    socketDStream.flatMap(_.split("\\W+")).map((_,1))
      .reduceByKeyAndWindow(_+_,_-_,Seconds(6),filterFunc = kv=>kv._2>0).print(1000)
    ssc.start()
    ssc.awaitTermination()
  }

}
