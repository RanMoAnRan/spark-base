package com.jing.sparkstreaming.receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * 自定义Receiver接收器接收数据，实现接口Receiver，从TCP Socket接收数据
  */
class CustomReceiver(hostname: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {

  // 启动接收器时，如何接收数据
  override def onStart(): Unit = {
    // 启动一个线程，一直接收数据
    new Thread(
      new Runnable {
        // 定义方法转换接收数据
        override def run(): Unit = receive()
      }
    ).start()
  }

  // 接收器停止接收数据的时候，如何处理
  override def onStop(): Unit = {

  }

  /**
    * 专门从TCP Socket中接收数据
    */
  private def receive(): Unit = {

    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(hostname, port)

      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      // TODO: 从字符流中一行一行的读取数据
      userInput = reader.readLine()
      // 使用while循环，判断是否读取导数据
      while (!isStopped && userInput != null) {
        // TODO: 将读取的数据进行存储，存储Executor中
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException => restart("Error connecting to " + hostname + ":" + port, e)
      case t: Throwable => restart("Error receiving data", t)
    }

  }
}
