package com.atguigu.spark.day01.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_SingleValue1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SingleValue").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list1: List[Int] = List(30,50,70,60,10,20)
    val rdd1: RDD[Int] = sc.parallelize(list1,3)

    //coalesce 缩减分区
//    println(rdd1.getNumPartitions)
//    val rdd2: RDD[Int] = rdd1.coalesce(2)
//    println(rdd2.getNumPartitions)
//    println(rdd1.getNumPartitions)
//    val rdd4: RDD[Int] = rdd1.repartition(3)
//    println(rdd4.getNumPartitions)

    //distinct


  }
}
