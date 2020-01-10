package com.atguigu.spark.day03_test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Cache {
  def main(args: Array[String]): Unit = {
val conf = new SparkConf().setAppName("Spark02_Cache").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List(30,40,50)
    val rdd: RDD[Int] = sc.parallelize(list,2)
    val rdd1: RDD[(Int, Int)] = rdd.map(x => {
      println(x + "--map")
      (x, 1)
    })
    val rdd2: RDD[(Int, Int)] = rdd1.filter(x => {
      println(x + "--filter")
      true
    })

//    val rdd3: RDD[(Int, Int)] = rdd2.combineByKey(s=>s,(_:Int)+(_:Int),(_:Int)+(_:Int))
//    rdd3.persist(StorageLevel.MEMORY_ONLY)
    //spark会自动对一些shuffle的过程进行缓存,来避免某个节点shuffle时坏掉导致重新从头计算RDD
//    val rdd3: RDD[(Int, Int)] = rdd2.repartition(3)
    val rdd3: RDD[(Int, Int)] = rdd2.mapValues(_+1)
    rdd3.collect().foreach(println)
    println("---------")
    rdd3.collect().foreach(println)
    Thread.sleep(30000000)
    sc.stop()
  }
}
