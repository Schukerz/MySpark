package com.atguigu.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("KafkaWordCount2")
    val params = Map[String,String](
      "group.id"->"group1",
      "bootstrap.servers" ->"hadoop102:9092,hadoop103:9092,hadoop104:9092"
    )

    val streamingContext = new StreamingContext(conf,Seconds(3))
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      streamingContext,
      params,
      Set[String]("hello")
    )
    kafkaDStream.map(_._2).flatMap(_.split("\\W +")).map((_,1)).reduceByKey(_+_).print()
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
