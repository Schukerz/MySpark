package com.atguigu.spark.day02.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark04_combineByKey {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("tuples").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
            val rrd2: RDD[(String, Int)] = rdd.combineByKey(x=>x, (_:Int) + (_:Int),(_:Int)+(_:Int))

    rrd2.collect.foreach(println)
    sc.stop()
  }
}
