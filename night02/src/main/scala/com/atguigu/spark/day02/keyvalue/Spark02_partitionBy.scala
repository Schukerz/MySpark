package com.atguigu.spark.day02.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark02_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hello").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List(30,40,50,10,70,60,10,20)
    val rdd: RDD[Int] = sc.parallelize(list,2)
    val rdd2: RDD[(Int, Int)] = rdd.map((_,1))
    val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new HashPartitioner(3))
    val rdd4: RDD[Array[(Int, Int)]] = rdd3.glom()
    rdd4.collect.foreach(arr=> println("aa"+arr.mkString(",")))
    sc.stop()

  }
}
