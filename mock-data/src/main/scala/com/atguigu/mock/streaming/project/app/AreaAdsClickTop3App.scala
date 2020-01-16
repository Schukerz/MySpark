package com.atguigu.mock.streaming.project.app

import com.atguigu.mock.streaming.project.bean.AdsInfo
import com.atguigu.mock.streaming.project.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object AreaAdsClickTop3App extends App {
  override def doSomething(stream: DStream[AdsInfo]): Unit ={
    val preKey = "day:area:ads:"
    stream.print()
    //1.每天每地区广告的点击量
    val dayAreaAdsAndCountDStream: DStream[((String, String, String), Int)] = stream.map(info => (info.dayString, info.area, info.adsId) -> 1)
      .updateStateByKey((seq:Seq[Int], opt:Option[Int]) => Some(opt.getOrElse(0) + seq.sum))

    //2.每天每地区分组
    val dayAreaAndAdsCountGrouped: DStream[((String, String), Iterable[(String, Int)])] = dayAreaAdsAndCountDStream.map {
      case ((day, area, ads), count) => ((day, area), (ads, count))
    }.groupByKey()

    //获取每天每地区前三
    val resultStream: DStream[((String, String), List[(String, Int)])] = dayAreaAndAdsCountGrouped.map {
      case ((day, area), adsCountIt) =>
        (day, area) -> adsCountIt.toList.sortBy(-_._2).take(3)
    }

    //存储结果到redis
    resultStream.foreachRDD(rdd =>{
      rdd.foreachPartition(( it)=>{

        //获取redis连接
        val client: Jedis = RedisUtil.getJedisClient
        it.foreach{
          case ((day,area),adsCountIt)=>{
            val key: String = preKey+day
            val field: String = area

            import org.json4s.JsonDSL._
            val value: String = JsonMethods.compact(JsonMethods.render(adsCountIt))
            client.hset(key,field,value)

          }
        }
        //关闭连接
        client.close()
      })
    })
  }
}
