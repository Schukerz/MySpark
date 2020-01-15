package com.atguigu.spark2

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.{SparkConf, streaming}

object MySourceDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mysource")
    val ssc = new StreamingContext(conf,streaming.Seconds(3))
    val ds: ReceiverInputDStream[String] = ssc.receiverStream[String](new MySource("hadoop102",9999))

    ds.flatMap(_.split("\\W+"))
      .map((_,1))
      .reduceByKey(_+_)
      .print(1000)

    ssc.start()
    ssc.awaitTermination()


  }
}
