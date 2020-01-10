package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark03_Accumulator").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val list = List(30,40,50,10,70,60,10,20)
        val a: LongAccumulator = sc.longAccumulator("first")
    val rdd: RDD[Int] = sc.parallelize(list,2)
    val rdd2: RDD[Int] = rdd.map(x => {
      a.add(1)
      x
    })
    rdd2.collect(    )
    println(a)
    sc.stop()
  }
}
