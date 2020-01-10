package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_TestAccumulator2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark05_TestAccumulator2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val myAcc = new Spark04_Accumulator2
    sc.register(myAcc,"first")
    val list = List(30,40,50,10,70,60,10,20)
    val rdd: RDD[Int] = sc.parallelize(list,2)
    rdd.map(x=>{
      myAcc.add(1)
      x
    }).collect()
    println(myAcc)
    sc.stop()

  }
}
