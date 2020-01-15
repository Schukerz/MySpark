package com.atguigu.spark2

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


class MySource(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  var socket : Socket= null
  var br: BufferedReader = null

  override def onStart(): Unit = {


      runInThread {
        try {
          socket = new Socket(host, port)
          br =
            new BufferedReader((new InputStreamReader(socket.getInputStream, "utf-8")))
          //以下语句一旦socket断开就结束程序了
//          var line: String = null
//          while (socket.isConnected && (line = br.readLine()) != null) {
//            store(line)
//          }
            var line: String = br.readLine()
            while (socket.isConnected &&line != null) {
              store(line)
              line=br.readLine()
            }


        } catch {
          case e => println(e.getMessage)
        } finally {

          restart("重启receiver")
        }
      }
  }

  override def onStop(): Unit = {
  if(socket!=null){
    socket.close()
  }
    if(br != null){
      br.close()
    }
  }
  def runInThread(f: =>Unit)={
    new Thread("run in a new thread"){
      override def run(): Unit = f
    }.start()
  }
}
