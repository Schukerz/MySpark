package com.atguigu.spark.app

import com.atguigu.spark.acc.CategoryAcc
import com.atguigu.spark.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CategoryTop10 {

  def statCategoryTop10(sc:SparkContext,userVisitActionRDD: RDD[UserVisitAction])={
    val acc = new CategoryAcc
    sc.register(acc,"CategoryAcc")
    userVisitActionRDD.foreach(u=>acc.add(u))
    val cidAndMap: Map[String, Map[(String, String), Long]] = acc.value.groupBy(_._1._1)
    val arrayCategory: Array[CategoryCountInfo] = cidAndMap.map {
      case (cid, map) => {
        CategoryCountInfo(cid, //如果又有click,又有order,会不会出现重复的情况
          map.getOrElse((cid, "click"), 0L),
          map.getOrElse((cid, "order"), 0L),
          map.getOrElse((cid, "pay"), 0L)
        )
      }
    }.toArray
    val resultCategory10: Array[CategoryCountInfo] = arrayCategory.sortBy(info => (-info.clickCount, -info.orderCount, -info.payCount))
      .take(10)
    resultCategory10
  }
}
