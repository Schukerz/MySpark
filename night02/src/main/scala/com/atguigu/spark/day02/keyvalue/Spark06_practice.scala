package com.atguigu.spark.day02.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_practice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark06_practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
      val rdd: RDD[String] = sc.textFile("x:\\spark\\agent.log")
    val proAdsToOne: RDD[((String, String), Int)] = rdd.map {
      t => {
        val split = t.split(" ")
        ((split(1), split(4)), 1)
      }
    }
    val proToAdsCountGrouped: RDD[(String, (String, Int))] = proAdsToOne.reduceByKey(_ + _).map {
      case ((pro, ads), count) => {
        (pro, (ads, count))
      }
    }
    proToAdsCountGrouped.collect.foreach(println)
    sc.stop()
  }

}
