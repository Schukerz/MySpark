package com.atguigu.night

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, streaming}

object Transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Tranform").setMaster("local[*]")
    val ssc = new StreamingContext(conf, streaming.Seconds(3))

    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val result: DStream[(String, Int)] = socketDStream.transform(rdd => {

      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    })
    result.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
