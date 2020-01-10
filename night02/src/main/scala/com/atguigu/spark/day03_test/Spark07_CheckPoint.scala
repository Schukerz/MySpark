package com.atguigu.spark.day03_test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_CheckPoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark07_CheckPoint").setMaster("local[2]")
        val sc = new SparkContext(conf)
        sc.setCheckpointDir("checkpoint")
        val list = List(30)
        val rdd: RDD[Int] = sc.parallelize(list,2)
        val rdd2: RDD[(Int, Long)] = rdd.map{
          println ("map")
          (_,System.currentTimeMillis())
        }
    //rdd2.checkpoint()
    val rdd3: RDD[(Int, Long)] = rdd2.filter{
      t=> {
        println("filter")
        true
      }
    }
    rdd3.cache()
    rdd3.checkpoint()
    println(rdd3.collect().mkString(","))
    println("------------")
    println(rdd3.collect().mkString(","))
    Thread.sleep(1000000)
    sc.stop()

  }
}
