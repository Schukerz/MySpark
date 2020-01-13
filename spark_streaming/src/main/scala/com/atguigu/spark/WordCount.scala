package com.atguigu.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    //StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(10))
    //DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val words: DStream[String] = lines.flatMap(_.split("""\s+"""))
    val result: DStream[(String, Int)] = words.map((_,1)).reduceByKey(_+_)
    println("aaa")
    result.print()
    ssc.start()
    //阻止程序退出,阻塞driver
    ssc.awaitTerminationOrTimeout(100000)

  }
}
