package com.jing.sparkstreaming.kafka.push

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Streaming实时统计每批次数据中词频统计WordCount, 从Kafka Topic中读取数据，采用push方式，将每批次结果打印到控制台。
  */
object StreamingKafkaPush {

  def main(args: Array[String]): Unit = {

    // TODO： 1、构建StreamingContext流式应用上下文实例对象
    // 设置应用的相关属性配置
    val conf: SparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    // 设置Streaming应用批处理时间间隔BatchInterval为5秒
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")


    // TODO: 2、从Kafka Topic中读取数据，采用push方式
    /*
      def createStream(
        ssc: StreamingContext,
        zkQuorum: String,
        groupId: String,
        topics: Map[String, Int],
        storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
      ): ReceiverInputDStream[(String, String)]
     */
    // Kafka Cluster 依赖的Zookeeper
    val zkQuorum: String = "hadoop01:2181,hadoop02:2181,hadoop03:2181"
    // 消费者组ID
    val groupId: String = "xxxx-id-0001"
    // Map集合，设置消费的topic相关信息，Map of (topic_name to numPartitions) to consume
    val topics: Map[String, Int] = Map("sparkTopic" -> 3)
    // 读取Kafka Topic中的数据，采用的事push方式
    val kafkaDStream: DStream[(String, String)] = KafkaUtils.createStream(
      ssc, zkQuorum, groupId, topics, StorageLevel.MEMORY_AND_DISK_SER
    )
    val inputDStream: DStream[String] = kafkaDStream.map(tuple => tuple._2)


    // TODO: 3、对流式的数据进行处理分析，调用DStream中函数
    /*
    def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
     */
    // 针对每批次数据的RDD进行操作
    val wordCountsDStream: DStream[(String, Int)] = inputDStream.transform(rdd =>
      // 此处不用判断RDD是否有值，因为此处针对RDD进行转换操作，不会触发Job执行，是Lazy操作
      rdd
        .filter(line => null != line && line.trim.split("\\s+").length > 0)
        .flatMap(line => line.trim.split("\\s+").filter(word => word.length > 0))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
    )


    /*
    def foreachRDD(foreachFunc: (RDD[T], Time) => Unit): Unit
     */
    wordCountsDStream.foreachRDD { (rdd, time) =>
      // 转换time格式为：2019/07/29 12:28:20
      val batchTime = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")
        .format(new Date(time.milliseconds))
      println("-------------------------------------------")
      println(s"Time: $batchTime")
      println("-------------------------------------------")
      // TODO：对DStream结果RDD输出的时候，要判断结果RDD是否存在
      if (!rdd.isEmpty()) {
        rdd
          // 降低分区数目
          .coalesce(1)
          .foreachPartition { datas =>
            datas.foreach(println)
          }
      }
    }


    // TODO: 5、针对流式应用需要启动程序
    ssc.start() // 此处启动接收器Receiver，用于从数据源端实时接收数据
    // 对于流式应用，通常情况下只要启动一直运行处理流式数据，除非认为关闭或者程序异常才终止，等待程序终止
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
