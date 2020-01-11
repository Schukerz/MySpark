package com.atguigu.spark.day01.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_DoubleValue {
  def main(args: Array[String]): Unit = {
  val conf: SparkConf = new SparkConf().setAppName("DoubleValue").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list1 = List(1,2,3,4,5,6)
    val list2 = List(3,4,5,6,7)
    val rdd1: RDD[Int] = sc.parallelize(list1,2)
    val rdd2: RDD[Int] = sc.makeRDD(list2,2)
//    val rdd3: RDD[Int] = rdd1.union(rdd2)

//    val rdd3: RDD[Int] = rdd1.intersection(rdd2)

//    val rdd3: RDD[Int] = rdd1.subtract(rdd2)
//    val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)
//    val rdd3: RDD[(Int, Long)] = rdd1.zipWithIndex()

//    val rdd3: RDD[(Int, Int)] = rdd1.zipPartitions(rdd2)((it1,it2)=>it1.zipAll(it2,-1,-2))

    val rdd3: RDD[(Int, Int)] = rdd1.cartesian(rdd2)


    rdd3.collect.foreach(println)
    Thread.sleep(1000000)
    sc.stop()

  }

}
