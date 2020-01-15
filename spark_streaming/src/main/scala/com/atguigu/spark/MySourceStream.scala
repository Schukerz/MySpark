package com.atguigu.spark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, streaming}

object MySourceStream {
    def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("mysource").setMaster("local[*]")
    val ssc = new StreamingContext(conf,streaming.Seconds(3))
    val ds: ReceiverInputDStream[String] = ssc.receiverStream(MySource("hadoop102",9999))
    val result: DStream[(String, Int)] = ds.flatMap(_.split("""\s+""")).map((_,1)).reduceByKey(_+_)
    println("ss")
    result.print
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000000)
    ssc.stop()
  }
}
