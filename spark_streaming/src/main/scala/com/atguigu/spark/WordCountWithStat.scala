package com.atguigu.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWithStat {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Stat")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("./ck1")
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val resultDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" ")).map((_,1))
//    resultDStream.reduceByKey(_+_).print
    val wordCountStream: DStream[(String, Int)] = resultDStream.updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
      Some(opt.getOrElse(0) + seq.sum)
    })
    wordCountStream.print
    ssc.start()
    ssc.awaitTermination()
  }
}
