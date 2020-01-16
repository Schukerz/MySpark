package com.atguigu.night

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object window1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Window")
    val ssc = new StreamingContext(conf,Seconds(3))
  val socketDStream: DStream[String] = ssc.socketTextStream("hadoop102",9999)
    .window(Seconds(6),Seconds(3))
    socketDStream.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }

}
