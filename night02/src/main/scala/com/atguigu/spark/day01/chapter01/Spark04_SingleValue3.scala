package com.atguigu.spark.day01.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_SingleValue3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("single3").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list: List[Int] = List(10,20,30,40,50,30,20,70)
    val rdd: RDD[Int] = sc.parallelize(list,3)
////    val rdd2: RDD[Int] = rdd.flatMap(s=>Array(s,s*s,s*s*s))
//    val rdd2: RDD[Int] = rdd.flatMap(s=>if(s>=50) Array(s) else Array[Int]() )
//    rdd2.collect.foreach(println)

//    val rdd2: RDD[Int] = rdd.map(math.pow(_,2).toInt)
//    rdd2.collect.foreach(println)


//    val rdd2: RDD[Int] = rdd.mapPartitions(x=>x.map(_+10))

    val rdd2: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex {
      (index, it) => it.map(x => (index, x))
    }
    rdd2

    rdd2.collect.foreach(println)

    sc.stop()
  }
}
