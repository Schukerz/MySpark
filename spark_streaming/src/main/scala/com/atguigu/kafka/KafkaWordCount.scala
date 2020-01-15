package com.atguigu.kafka


import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, streaming}

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("kafka").setMaster("local[*]")
    val ssc = new StreamingContext(conf,streaming.Seconds(3))

    //[K, V, KD <: Decoder[K], VD <: Decoder[V]]
   /*
   jssc: JavaStreamingContext,
    keyClass: Class[K],
    valueClass: Class[V],
    keyDecoderClass: Class[KD],
    valueDecoderClass: Class[VD],
    kafkaParams: JMap[String, String],
    topics: JSet[String]
    */

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
    ds.map(_._2).flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_).print(1000)
    ssc.start()
    ssc.awaitTerminationOrTimeout(1000000)

  }

}
