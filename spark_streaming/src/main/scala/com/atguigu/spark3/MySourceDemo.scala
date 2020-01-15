package com.atguigu.spark3

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.{SparkConf, streaming}



object MySourceDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MySource")
    val streamingContext: StreamingContext = new StreamingContext(conf,streaming.Seconds(3))
    val MySourceDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MySource("hadoop102",9999))
//val MySourceDStream=streamingContext.socketTextStream("hadoop102",9999)
    val mapDStream: DStream[(String, Int)] = MySourceDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    mapDStream.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
class MySource(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  var socket:Socket = _
  var reader : BufferedReader = _

  override def onStart(): Unit = {
    newThreader{
      try{
        socket = new Socket(host,port)
        reader = new BufferedReader(
          new InputStreamReader(socket.getInputStream,"utf-8"))
        var line: String = reader.readLine()
        while(line != null && socket.isConnected){
          store(line)
          line = reader.readLine()
        }
      }catch {
        case e :Exception => println(e.getMessage)
      }finally {
        restart("连接失效,正在重启")
      }

    }
  }

  override def onStop(): Unit = {
    if(socket != null){
      socket.close()
    }
    if(reader != null){
      reader.close()
    }
  }

  def newThreader(f: =>Unit)={
    new Thread("run in a new Thread"){
      override def run(): Unit = f
    }.start()
  }
}
