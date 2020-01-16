package com.mycompany.streaming.utils

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object MyKafkaUtil {
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "bigdata",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def createKafkaDStream(ssc:StreamingContext,topic:String,otherTopic:String*)={
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](otherTopic :+ topic, kafkaParams)
    )
    stream.map(record =>record.value)
  }



}
