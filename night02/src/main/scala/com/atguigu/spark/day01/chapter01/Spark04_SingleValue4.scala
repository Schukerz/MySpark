package com.atguigu.spark.day01.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_SingleValue4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("single4").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List(1,2,5,3,6,4,4)
    val rdd: RDD[Int] = sc.parallelize(list,2)
//    val rddw: RDD[Array[Int]] = rdd.glom()
//    rddw.collect.foreach(x=> println(x.mkString(",")))

//    val rdd2: RDD[(Int, Iterable[Int])] = rdd.groupBy(s=>s%2)
//    val rdd2: RDD[Int] = rdd.sample(true,1.1)
//    val rdd2: RDD[Int] = rdd.repartition(4)
//    println(rdd2.partitions.length)

    val rdd2: RDD[Int] = rdd.sortBy(s=>s,true)
    rdd2.collect.foreach(println)
    
//    rdd2.collect

    sc.stop()

  }
}
