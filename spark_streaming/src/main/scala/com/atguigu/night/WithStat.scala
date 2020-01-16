package com.atguigu.night

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WithStat {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("stat").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("./ck")
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val mapDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_)
    val wordAndCount: DStream[(String, Int)] = mapDStream.updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
      Some(opt.getOrElse(0) + seq.sum)
    })
    wordAndCount.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
