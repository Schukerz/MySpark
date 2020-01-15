package com.atguigu.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCoundWindow {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    //StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(2))
    ssc.checkpoint("./ck")
    //DStream
//    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

//    val words: DStream[String] = lines.flatMap(_.split("""\s+"""))
//    val result: DStream[(String, Int)] = words.map((_,1))
//      //.reduceByKeyAndWindow(_+_,Seconds(6),slideDuration = Seconds(2))
//      .reduceByKeyAndWindow(_+_,_-_,Seconds(6),filterFunc = kv=>kv._2>0)
  val lines: DStream[String] = ssc.socketTextStream("hadoop102",9999).window(Seconds(6),Seconds(4))
    lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()


    ssc.start()
    //阻止程序退出,阻塞driver
    ssc.awaitTermination
  }

}
