package com.atguigu.streaming.project.app
import com.atguigu.streaming.project.bean.AdsInfo
import com.atguigu.streaming.project.utils.RedisUtil
import org.apache.spark.streaming
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/*
需求:最近 1 小时广告点击量实时统计
各广告最近 1 小时内各分钟的点击量
 */
object LastHourAdsClickApp extends App {
  override def doSomething(sourceDStream: DStream[AdsInfo]): Unit = {
    val preKey:String = "last:ads:hour"
    //1.划分窗口
    val adsInfoDStreamWithWindow: DStream[AdsInfo] = sourceDStream.window(streaming.Minutes(60))
    //2.转换操作
    val adsAndHmCountGrouped: DStream[(String, Iterable[(String, Int)])] = adsInfoDStreamWithWindow
      .map(info => ((info.adsId, info.hmString), 1))
      .reduceByKey(_ + _)
      .map {
        case ((ads, hm), count) => (ads, (hm, count))
      }
      .groupByKey()
    //写入redis
    //单独一个写入redis
//    adsAndHmCountGrouped.foreachRDD(rdd=>{
//      rdd.foreachPartition(it=>{
//        val client: Jedis = RedisUtil.getJedisClient
//        val list: List[(String, Iterable[(String, Int)])] = it.toList
//        list.foreach{
//          case (ads,iter)=>{
//            val key :String = preKey
//            val value: String = JsonMethods.compact(JsonMethods.render(iter))
//            println(value)
//            client.hset(key,ads,value)
//
//          }
//        }
//        client.close()
//      })
//    })

    //批量写入redis
    import org.json4s.JsonDSL._
    import scala.collection.JavaConversions._
    adsAndHmCountGrouped.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        val list: List[(String, Iterable[(String, Int)])] = it.toList
        if(list.size > 0 ){
          val client: Jedis = RedisUtil.getJedisClient
          val map: Map[String, String] = list.toMap.map {
            case (ads, iter) => {
              (ads, JsonMethods.compact((ads, JsonMethods.render(iter))))
            }
          }
          client.hmset("last:ads:hour",map)
          client.close()
        }

      })
    })

  }
}
