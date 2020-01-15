package com.atguigu.practice

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, streaming}

object Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val streamingContext: StreamingContext = new StreamingContext(conf,streaming.Seconds(3))
    val socketLineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102",9999)
    val wordDStream: DStream[String] = socketLineDStream.flatMap(_.split(" "))
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_,1)).reduceByKey(_+_)
    mapDStream.print(1000)

    println("hello")
    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(100000)
//    while(true){
//      Thread.sleep(1000)
//    }

  }
}
