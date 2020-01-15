package com.atguigu.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, streaming}

object CheckPoint {
  def createSSC()={
    val conf: SparkConf = new SparkConf().setAppName("SSC").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,streaming.Seconds(3))
    ssc.checkpoint("/home/atguigu/ck")
    val KafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      Map[String, String](("group.id", "morning"),
        ("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")),
      Set[String]("hello")
    )
    val resultDStream: DStream[(String, Int)] = KafkaDStream.flatMap {
      case (_, v) => v.split(" ").map((_, 1))
    }.reduceByKey(_ + _)
    resultDStream.print()
    ssc
  }
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("/home/atguigu/ck",createSSC)
    ssc.start()
    ssc.awaitTermination()

  }

}
