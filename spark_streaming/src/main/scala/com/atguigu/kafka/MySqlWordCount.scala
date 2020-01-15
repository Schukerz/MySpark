package com.atguigu.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, streaming}

object MySqlWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("kafka").setMaster("local[*]")
    val ssc = new StreamingContext(conf,streaming.Seconds(3))
    val params = Map[String,String](
         "group.id" ->"morning3",
      "bootstrap.servers"->"hadoop102:9092,hadoop103:9092,hadoop104:9092"
    )
    val ds: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      params,
      Set[String]("hello11")
    )
    val kafkaDStream: DStream[(String, Int)] = ds.map(_._2).flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_)

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000000)

  }

}
