package com.atguigu.spark.day01.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_wc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("a").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List(("a",1),("b",2),("c",3),("a",4),("b",5),("c",6),("c",7),("c",8))
    val rdd: RDD[(String, Int)] = sc.parallelize(list,2)

//    val rdd2: RDD[(String, Int)] = rdd.reduceByKey(_+_)
//    val rdd2: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
//    val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)
//    val rdd2: RDD[(String, Int)] = rdd.groupByKey().map {
//      case (word, num) => (word, num.sum)
//    }
    val rdd2: RDD[(String, Int)] = rdd.groupBy(_._1).map {
      case (word, num) => (word, num.map(_._2).sum)
//      case (word, num) => (word, num.foldLeft(0)((a,b)=>a._+b._2))
    }

    //combineByKeyClassTag
    rdd2.collect.foreach(println)

    sc.stop()
  }
}
