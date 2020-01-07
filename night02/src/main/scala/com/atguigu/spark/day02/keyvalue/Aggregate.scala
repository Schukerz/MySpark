package com.atguigu.spark.day02.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Aggregate {
  def main(args: Array[String]): Unit = {
   val conf: SparkConf = new SparkConf().setAppName("tuples").setMaster("local[2]")
   val sc: SparkContext = new SparkContext(conf)
   val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    val rddglom: RDD[Array[(String, Int)]] = rdd.glom()
    rddglom.collect.foreach(x=> println(x.mkString(",")))
//    val result: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)
//    val result: RDD[(String, Int)] = rdd.aggregateByKey(Int.MinValue)(_.max(_),_+_)
    //每个key每个分区的最大值和最小值之和
    val result: RDD[(String, (Int, Int))] = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))({
      case ((max, min), v) => {
        (max.max(v), min.min(v))
      }
    }, {
      case ((max1, min1), (max2, min2)) => {
        (max1 + max2, min1 + min2)
      }
    })
    //求每个key的平均值
    val result2: RDD[(String, Double)] = rdd.aggregateByKey((0, 0))({
      case ((sum, count), v) => (sum + v, count + 1)
    }, {
      case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
    }
    ).map { case (word, (sum, count)) => (word, sum.toDouble / count) }
    result2.collect.foreach(println)
    sc.stop()
  }
}
