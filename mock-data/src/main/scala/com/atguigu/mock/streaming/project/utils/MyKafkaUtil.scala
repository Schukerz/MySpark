package com.atguigu.mock.streaming.project.utils

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object MyKafkaUtil {


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "bigdata",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )


  def getKafkaStream(ssc:StreamingContext,topic : String,otherTopics:String*)= {
    var topics = Array(topic) ++ otherTopics
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record =>record.value())
  }
}
