package com.atguigu.mock.streaming.project.app

import com.atguigu.mock.streaming.project.bean.AdsInfo
import com.atguigu.mock.streaming.project.utils.RedisUtil
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

//最近 1 小时广告点击量实时统计
/*
1.划分窗口
2.跳转数据类型((ads,hm),1)
3.统计
4.写入到redis
 */
object LastHourAdsClickApp extends App {
  override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
    val adsInfoStreamWithWindow: DStream[AdsInfo] = adsInfoStream.window(Minutes(2))
    val adsHmCountGrouped: DStream[(String, Iterable[(String, Int)])] = adsInfoStreamWithWindow.map(info => ((info.adsId, info.hmString), 1))
      .reduceByKey(_ + _)
      .map {
        case ((ads, hm), count) => (ads, (hm, count))
      }
      .groupByKey()
    adsHmCountGrouped.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{

        import org.json4s.JsonDSL._
        import scala.collection.JavaConversions._
        val list: List[(String, Iterable[(String, Int)])] = it.toList
        if(list.size>0){
          val client: Jedis = RedisUtil.getJedisClient
          val key: String = "last:ads:hour"
          val map: Map[String, String] = list.toMap.map {
            case (adsId, iter) =>
              (adsId,JsonMethods.compact(JsonMethods.render(iter)))
          }
          println(map)
          client.hmset(key,map)
          //关闭连接
          client.close

        }

      })
    })


  }
}

