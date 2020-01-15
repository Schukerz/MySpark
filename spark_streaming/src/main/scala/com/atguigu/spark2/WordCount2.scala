package com.atguigu.spark2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object WordCount2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wc2").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val sc: SparkContext = ssc.sparkContext
    val queue = new mutable.Queue[RDD[Int]]()
    val ds: InputDStream[Int] = ssc.queueStream[Int](queue,true)
    ds.reduce(_+_).print(100)
    ssc.start()
    for(i <- 1 to 10){
      val rdd: RDD[Int] = sc.makeRDD(1 to 10)
      queue.enqueue(rdd)
      println(queue.length)
    }
    ssc.awaitTerminationOrTimeout(100000)

  }
}
