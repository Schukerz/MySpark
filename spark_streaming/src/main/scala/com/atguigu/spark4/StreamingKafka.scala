package com.atguigu.spark4

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, streaming}

object StreamingKafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreaming")
    val streamingContext: StreamingContext = new  StreamingContext(conf,streaming.Seconds(3))
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      streamingContext,
      Map[String, String](("group.id", "morning"),
        ("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")),
      Set[String]("hello")
    )
    val mapDStream: DStream[(String, Int)] = kafkaDStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    mapDStream.print()
    streamingContext.start()
    streamingContext.awaitTermination()
//    ConsumerConfig
  }
}
