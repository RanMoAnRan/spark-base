package com.jing.spark.state

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 仿双十一实时统计各个省份销售订单额，从Kafka Topic（`orderTopic`）中读取数据，
  * 每个订单数据中各个字段信息使用逗号隔开（orderId,provinceId,orderPrice）
  * 状态统计updateStateByKey API（mapWithState已经代替了此api）
  * 并且设置检查点
  */
object StreamingOrderAmtCkpt02 {

  // Streaming检查点目录
  val CKPT_DIR: String = "datas/streaming/state/order-amt-1222221"

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
    val orderDStream: DStream[(Int, Double)] = kafkaDStream.transform(rdd => {
      // 数据格式：orderId,provinceId,orderPrice
      val orderRDD = rdd
        .filter(msg => null != msg._2 && msg._2.trim.split(",").length >= 3)
        .mapPartitions { iter =>
          iter.map { case (_, value) =>
            val Array(_, provinceId, orderPrice) = value.trim.split(",")
            // 返回二元组(省份ID, 订单金额）
            (provinceId.toInt, orderPrice.toDouble)
          }
        }
      orderRDD
    })

    /*
      依据每批次中Key来统计和更新状态信息（当前批次中Key的状态和以前的状态）
     */
    val provinceOrderAmtTotalDStream: DStream[(Int, Double)] = orderDStream.updateStateByKey(
      // (Seq[V], Option[S]) => Option[S]
      (values: Seq[Double], state: Option[Double]) => {
        // a. 获取当前批次中Key的状态（应用当前批次中省份销售订单额）
        val currentOrderAmt: Double = values.sum
        // b. 获取Key以前的状态（订单销售额）
        val previousOrderAmt: Double = state.getOrElse(0.0)
        // c. 聚合 最新Key的状态
        val lastestOrderAmt = currentOrderAmt + previousOrderAmt
        // d. 返回最新状态
        Some(lastestOrderAmt)
      }
    )

    // TODO: 4、将每批次结果RDD进行输出，调用foreachRDD函数
    provinceOrderAmtTotalDStream.foreachRDD { (rdd, time) =>
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

    val context = StreamingContext.getOrCreate(
      CKPT_DIR, //
      () => {
        // 设置应用的相关属性配置
        val conf: SparkConf = new SparkConf()
          .setMaster("local[3]")
          .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
          .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        // 设置Streaming应用批处理时间间隔BatchInterval为5秒
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        // TODO: 设置检查点目录
        ssc.checkpoint(CKPT_DIR)

        // 调用Streaming数据处理函数即可，当再次运行Streaming应用的时候，从检查点获取如何读取数据，如何处理数据
        processStreaming(ssc)

        // 返回
        ssc
      }
    )
    context.sparkContext.setLogLevel("WARN")


    // TODO: 5、针对流式应用需要启动程序
    context.start()
    context.awaitTermination()


    //context.awaitTerminationOrTimeout(10000)
    context.stop(stopSparkContext = true, stopGracefully = true)
  }

}
