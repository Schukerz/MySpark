package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, streaming}

import scala.collection.mutable

object RDDQueueStream {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Queue").setMaster("local[*]")

    val ssc = new StreamingContext(conf,streaming.Seconds(1))
    val sc = ssc.sparkContext
    val queue = new mutable.Queue[RDD[Int]]()
    val rrdDS: InputDStream[Int] = ssc.queueStream(queue,true)
    rrdDS.reduce(_+_).print(100)
    ssc.start()

    for(i <- 1 to 10){
      queue.enqueue(sc.makeRDD(1 to 100))
      //Thread.sleep(2000)
      println(queue.length)
    }
    ssc.awaitTerminationOrTimeout(100000)


  }
}
