package com.atguigu.streaming.project.utils

import org.apache.spark.streaming.StreamingContext

object KafkaUtil {
def getKafkaConsumer(ssc:StreamingContext,topic:String,otherTopics:String*)={
  import org.apache.kafka.common.serialization.StringDeserializer
  import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
  import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
  import org.apache.spark.streaming.kafka010._

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "happyNewYear",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )


  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](otherTopics :+ topic, kafkaParams)
  )

  stream.map(record =>  record.value)
}
}
