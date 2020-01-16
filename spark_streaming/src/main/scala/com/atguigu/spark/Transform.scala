package com.atguigu.spark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.{SparkConf, streaming}

object Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tr")
    val ssc = new StreamingContext(conf,streaming.Seconds(3))
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    socketDStream.transform(rdd=>{
     rdd.flatMap(_.split(" ").map((_,1))).reduceByKey(_+_)
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
