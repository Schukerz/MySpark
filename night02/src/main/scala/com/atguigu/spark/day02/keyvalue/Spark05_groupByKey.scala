package com.atguigu.spark.day02.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Spark05_groupByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
//    val result: RDD[(String, Iterable[Int])] = rdd.groupBy(_._1).map {
//      case (word, it) => {
//        (word, it.map(_._2))
//      }
//    }
    val result: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    result.collect.foreach(println)
    sc.stop()
  }
}
