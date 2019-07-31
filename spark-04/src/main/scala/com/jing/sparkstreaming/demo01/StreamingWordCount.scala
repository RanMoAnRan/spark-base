package com.jing.sparkstreaming.demo01

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Streaming实时统计每批次数据中词频统计WordCount, 从TCP Socket中读取数据，将每批次结果打印到控制台。
  */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    // TODO： 1、构建StreamingContext流式应用上下文实例对象
    // 设置应用的相关属性配置
    val conf: SparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("StreamingWordCount")
    // 设置Streaming应用批处理时间间隔BatchInterval为5秒
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")


    // TODO: 2、从TCP Socket读取数据
    /*
      def socketTextStream(
        hostname: String,
        port: Int,
        storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
      ): ReceiverInputDStream[String]
     */
    val inputDStream: DStream[String] = ssc.socketTextStream(
      "hadoop02", 9999, storageLevel = StorageLevel.MEMORY_AND_DISK)


    // TODO: 3、对流式的数据进行处理分析，调用DStream中函数
    val wordCountsDStream: DStream[(String, Int)] = inputDStream
      // 将每条数据按照空格进行分割为单词
      .flatMap(line => line.trim.split("\\s+").filter(word => word.length > 0))
      // 将每个单词转换为二元组，表示此单词出现一次
      .map(word => (word, 1))
      // 按照单词分组统计出现次数
      .reduceByKey((a, b) => a + b)


    // TODO: 4、将结果数据进行输出到控制台
    wordCountsDStream.print(10)


    // TODO: 5、针对流式应用需要启动程序
    ssc.start() // 此处启动接收器Receiver，用于从数据源端实时接收数据
    // 对于流式应用，通常情况下只要启动一直运行处理流式数据，除非认为关闭或者程序异常才终止，等待程序终止
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
