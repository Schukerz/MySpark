package com.atguigu.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark01_Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List(30,40,50,10,70,60,10,20)
    val rdd: RDD[Int] = sc.parallelize(list,2)
    rdd.collect().foreach(println)
    Thread.sleep(1000000)
    sc.stop()
  }

}
