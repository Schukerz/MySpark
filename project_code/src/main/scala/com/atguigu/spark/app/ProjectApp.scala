package com.atguigu.spark.app

import com.atguigu.spark.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sourceRDD: RDD[String] = sc.textFile("X:\\spark\\user_visit_action.txt")
    val userVisitActionRDD: RDD[UserVisitAction] = sourceRDD.map(line => {
      val splits: Array[String] = line.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong)

    })


    //需求 查找热度前10的品类
    val categoryCountInfo: Array[CategoryCountInfo] = CategoryTop10.statCategoryTop10(sc,userVisitActionRDD)
    categoryCountInfo.foreach(println)

  }
}
