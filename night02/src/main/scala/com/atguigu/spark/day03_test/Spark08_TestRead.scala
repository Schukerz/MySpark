package com.atguigu.spark.day03_test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_TestRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark08_TestRead").setMaster("local[1]")
        val sc = new SparkContext(conf)
        val rdd: RDD[String] = sc.textFile("in",1)
        println(rdd.partitions.size)
        println("+++++++++++++++++")
        println(rdd.getNumPartitions)

        val rdd2: RDD[String] = rdd.coalesce(2)
    println(rdd2.getNumPartitions)
    val rdd3: RDD[Unit] = rdd2.mapPartitions(it => {
      println("--")
      it.map(println)
    })
    rdd3
    rdd3.collect().foreach(println)


  }
}
