package com.atguigu.spark.day01.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_AggregateByKey2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("a").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List(("a",1),("b",2),("c",3),("a",4),("b",5),("c",6),("c",7),("c",8))
    val rdd: RDD[(String, Int)] = sc.parallelize(list,2)
    val rdd3: RDD[Array[(String, Int)]] = rdd.glom()
    rdd3.collect.foreach(x=>println(x.mkString(",")))
//    val result: RDD[(String, (Int, Int))] = rdd.aggregateByKey((Int.MaxValue, Int.MinValue))(
//      { case ((min, max), v) => {
//        (min.min(v), max.max(v))
//      }
//      }, {
//        case ((min1, max1), (min2, max2)) => {
//          (min1 + min2, max1 + max2)
//        }
//      }
//    )
//    result.collect.foreach(println)

    //求每个key的平均值
    val tmp: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      {
        case ((sum, count), v) => (sum + v, count + 1)
      },
      {
        case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
      }
    )
    tmp.collect.foreach(println)
    val result2: RDD[(String, Double)] = tmp.map { case (k, (sum, count)) => (k, sum.toDouble / count) }
    result2.collect.foreach(println)

    sc.stop()
  }
}
