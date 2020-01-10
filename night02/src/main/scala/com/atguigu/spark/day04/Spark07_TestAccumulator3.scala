package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_TestAccumulator3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark07_TextAccumulator3").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val acc = new Spark06_Accumulator3
    sc.register(acc,"MapAcc")
    val list: List[(String, Int)] = List(("a",1),("b",1),("c",2),("d",2))
    val rdd: RDD[(String, Int)] = sc.parallelize(list,2)
    rdd.map(x=>{
      acc.add(x._2)
      x
    }).collect()
    println(acc)
    sc.stop()

    

  }
}
