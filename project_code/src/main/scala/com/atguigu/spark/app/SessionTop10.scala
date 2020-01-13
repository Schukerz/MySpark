package com.atguigu.spark.app

import com.atguigu.spark.bean.{CategoryCountInfo, SessionTreeset, UserVisitAction}
import org.apache.spark.rdd.RDD

object SessionTop10 {
def getSessionTop10(userVisitActionRDD: RDD[UserVisitAction],categoryCountInfo: Array[CategoryCountInfo])={

  //获取所有cids
  val cids: Array[String] = categoryCountInfo.map(_.categoryId)
  //过滤
  userVisitActionRDD.filter(action=>cids.contains(action.click_category_id.toString))
    .map(action =>((action.click_category_id,action.session_id),1))
    .reduceByKey(_+_)
    .groupBy(_._1._1)
    .mapValues{
      case ((cid,session),count)=>{
        SessionTreeset(cid,session,count)
      }
    }

}
}
