package com.atguigu.streaming.project.app
import com.atguigu.streaming.project.bean.AdsInfo
import com.atguigu.streaming.project.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

//需求:每天每地区热门广告 Top3

object AreaAdsTop3App extends App {
  override def doSomething(sourceDStream: DStream[AdsInfo]): Unit = {
    val preKey: String = "day:area:ads:"
    //1.截取字段
    val dayAreaAdsAndOne: DStream[((String, String, String), Int)] = sourceDStream
        .map(info=>((info.dayString,info.area,info.adsId),1))
    //2.累加数据
    val dayAreaAdsAndCount: DStream[((String, String, String), Int)] = dayAreaAdsAndOne
        .updateStateByKey((seq:Seq[Int],opt:Option[Int])=>Some(opt.getOrElse(0)+seq.sum))
    //3.转换分组
    val dayAreaAndAdsCountGrouped: DStream[((String, String), Iterable[(String, Int)])] = dayAreaAdsAndCount.map {
      case ((day, area, ads), count) => ((day, area), (ads, count))
    }.groupByKey()
    //4.获取前三
    val dayAreaAndAdsCountTop3: DStream[((String, String), List[(String, Int)])] = dayAreaAndAdsCountGrouped.mapValues(it => {
      it.toList.sortBy(-_._2).take(3)
    })

    //5.把结果写入redis
    dayAreaAndAdsCountTop3.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        //建立Jedis连接
        val client: Jedis = RedisUtil.getJedisClient
        //遍历it,把数据写入redis
        it.foreach{
          case ((day,area),list)=>{

            //获取redis中的key
            val key :String = preKey + day
            //获取redis中的field
            val field:String = area
            //获取redis中的value
            import org.json4s.JsonDSL._
            val value: String = JsonMethods.compact(JsonMethods.render(list))
            client.hset(key,field,value)

          }
        }
        client.close()

      })
    })






  }
}
