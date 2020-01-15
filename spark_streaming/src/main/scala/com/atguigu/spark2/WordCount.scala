package com.atguigu.spark2

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, streaming}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,streaming.Seconds(3))
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val result: DStream[(String, Int)] = ds.flatMap(_.split("""\\w+""")).map((_,1)).reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(100000)
    ssc.stop()
  }

}
