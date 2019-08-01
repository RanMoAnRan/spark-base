package com.jing.spark.window

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 仿双十一实时统计各个省份最近订单数目，从Kafka Topic（`orderTopic`）中读取数据，
  * 每个订单数据中各个字段信息使用逗号隔开（orderId,provinceId,orderPrice）
  * 窗口统计
  */
object StreamingOrderReduceWindow {

  // Streaming检查点目录
  val CKPT_DIR: String = "datas/streaming/state/order-amt-1000011155555555552"

  // 时间间隔(批次、窗口、滑动）
  val STREAMING_BATCH_INTERVAL = 2
  val STREAMING_WINDOW_INTERVAL = STREAMING_BATCH_INTERVAL * 2
  val STREAMING_SLIDER_INTERVAL = STREAMING_BATCH_INTERVAL * 1

  /**
    * 将Streaming流式应用从数据源数据，数据处理分析及结构数据输出封装到函数㕜
    *
    * @param ssc 流式应用上下文实例的
    */
  def processStreaming(ssc: StreamingContext): Unit = {
    // TODO: 2、从Kafka Topic中读取数据，采用direct方式
    val kafkaParams: Map[String, String] = Map(
      "bootstrap.servers" -> "hadoop01:9092,hadoop01:9092,hadoop01:9092",
      "auto.offset.reset" -> "largest"
    )
    val topics: Set[String] = Set("orderTopic")
    val kafkaDStream: DStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics
    )


    // TODO: 3、对流式的数据进行处理分析，调用DStream中函数
    val orderDStream: DStream[(String, Int)] = kafkaDStream.transform(rdd => {
      // 数据格式：orderId,provinceId,orderPrice
      rdd
        .filter(msg => null != msg._2 && msg._2.trim.split(",").length >= 3)
        .mapPartitions { iter =>
          iter.map { case (_, value) =>
            val Array(_, provinceId, _) = value.trim.split(",")
            // 返回二元组(省份ID, 订单次数）
            (provinceId, 1)
          }
        }
    })



    /*
    def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration
    ): DStream[(K, V)]

     */
    // 此处调用窗口聚合函数，进行窗口操作
    val orderCountDStream: DStream[(String, Int)] = orderDStream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2,
      Seconds(STREAMING_WINDOW_INTERVAL), // 窗口大小
      Seconds(STREAMING_SLIDER_INTERVAL) // 滑动大小，时间间隔
    )



    // TODO: 4、将每批次结果RDD进行输出，调用foreachRDD函数
    orderCountDStream.foreachRDD { (rdd, time) =>
      val batchTime = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss").format(new Date(time.milliseconds))
      println("-------------------------------------------")
      println(s"Time: $batchTime")
      println("-------------------------------------------")
      // TODO：对DStream结果RDD输出的时候，要判断结果RDD是否存在
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).foreachPartition { datas => datas.foreach(println) }
      }
    }
  }

  def main(args: Array[String]): Unit = {

    // TODO： 1、构建StreamingContext流式应用上下文实例对象
    /*
      def getOrCreate(
          // 检查点目录，Streaming应用第一次运行，
          // 目录不存在，调用creatingFunc函数构建StreamingContext实例对象；如果存在从检查点目录构建
        checkpointPath: String,
        // 目录不存在，调用creatingFunc函数构建StreamingContext实例对象
        creatingFunc: () => StreamingContext,
        hadoopConf: Configuration = SparkHadoopUtil.get.conf,
        createOnError: Boolean = false
      ): StreamingContext
     */
    val context = StreamingContext.getOrCreate(
      CKPT_DIR, // get表示检查点目录存在，就从检查目录中的数据恢复，没有的话调用下面函数创建
      () => {
        // 设置应用的相关属性配置
        val conf: SparkConf = new SparkConf()
          .setMaster("local[3]")
          .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
          .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        // 设置Streaming应用批处理时间间隔BatchInterval为5秒
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(STREAMING_BATCH_INTERVAL))

        // TODO: 设置检查点目录
        ssc.checkpoint(CKPT_DIR)

        // 调用Streaming数据处理函数即可，当再次运行Streaming应用的时候，从CKPt获取如何读取数据，如何处理数据
        processStreaming(ssc)

        // 返回
        ssc
      }
    )
    context.sparkContext.setLogLevel("WARN")


    // TODO: 5、针对流式应用需要启动程序
    context.start()
    context.awaitTermination()
    context.stop(stopSparkContext = true, stopGracefully = true)
  }

}
