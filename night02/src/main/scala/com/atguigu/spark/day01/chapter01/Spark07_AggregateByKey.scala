package com.atguigu.spark.day01.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("a").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List(("a",1),("b",2),("c",3),("a",4),("b",5),("c",6),("c",7),("c",8))
    val rdd: RDD[(String, Int)] = sc.parallelize(list,2)
    val rdd3: RDD[Array[(String, Int)]] = rdd.glom()
    rdd3.collect.foreach(x=>println(x.mkString(",")))
    val result: RDD[(String, (Int, Int))] = rdd.aggregateByKey((Int.MaxValue, Int.MinValue))({
      case ((a, b), c) => (a.min(c), b.max(c))
    },
      { case ((min1, max1), (min2, max2)) => (min1 + min2, max1 + max2)
      })
    result.collect.foreach(println)
    sc.stop()
  }
}
