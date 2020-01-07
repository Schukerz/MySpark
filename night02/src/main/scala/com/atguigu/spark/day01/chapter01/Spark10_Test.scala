package com.atguigu.spark.day01.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("X:\\spark\\agent.log")
    val proAndAdsToOne: RDD[((String, String), Int)] = rdd.map {
      line => {
        val split: Array[String] = line.split(" ")
        ((split(1), split(4)), 1)
      }
    }
    val proAndAdsToCount: RDD[((String, String), Int)] = proAndAdsToOne.reduceByKey(_+_)
    val proToAdsAndCountGrouped: RDD[(String, Iterable[(String, Int)])] = proAndAdsToCount.map {
      case ((pro, ads), count) => (pro, (ads, count))
    }.groupByKey()
    val result: RDD[(String, List[(String, Int)])] = proToAdsAndCountGrouped.map {
      case (pro, it) => {
        (pro, it.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
      }
    }.sortBy(_._1.toInt)
    result.collect.foreach(println)

    val result2: RDD[(String, List[(String, Int)])] = rdd
      .map(_.split(" ")).map(t => ((t(1), t(4)), 1))
      .reduceByKey(_ + _).map(t => (t._1._1, (t._1._2, t._2)))
      .groupByKey().mapValues(_.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)).sortBy(_._1.toInt)
    result2.collect.foreach(println)
    sc.stop()
  }
}
