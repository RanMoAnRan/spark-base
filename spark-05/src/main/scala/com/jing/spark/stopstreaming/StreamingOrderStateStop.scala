package com.jing.spark.stopstreaming

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * 仿双十一实时统计各个省份销售订单额，从Kafka Topic（`orderTopic`）中读取数据，
  * 每个订单数据中各个字段信息使用逗号隔开（orderId,provinceId,orderPrice）
  */
object StreamingOrderStateStop {

  // Streaming检查点目录, 此时表示HDFS文件系统目录
  val CKPT_DIR: String = "/datas/streaming/state/order-amt-state-0000441"

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
      rdd
        .filter(msg => null != msg._2 && msg._2.trim.split(",").length >= 3)
        .mapPartitions { iter =>
          iter.map { case (_, value) =>
            val Array(_, provinceId, orderPrice) = value.trim.split(",")
            // 返回二元组(省份ID, 订单金额）
            (provinceId.toInt, orderPrice.toDouble)
          }
        }
        // TODO： 当使用状态更新函数的时候，应该先对每批次数据按照Key进行聚合操作，在进行状态更新
        .reduceByKey((a, b) => a + b)
    })


    /*
      此更新函数，同样也是按照Key更新状态，将状态更新操作封装到类中StateSpec，调用类中函数更新状态
        def mapWithState[StateType: ClassTag, MappedType: ClassTag](
        spec: StateSpec[K, V, StateType, MappedType]
      ): MapWithStateDStream[K, V, StateType, MappedType]

      a. 更新函数
        def function[KeyType, ValueType, StateType, MappedType](
          mappingFunction: (KeyType, Option[ValueType], State[StateType]) => MappedType
        ): StateSpec[KeyType, ValueType, StateType, MappedType]

      b. 状态封装类的泛型
        K: Key的类型，针对应用来说就是各个省份ID
        V：DStream中Value类型，订单销售额
        StateType：状态类型，针对应用来说订单销售额，Double类型
        MappedType：mappingFunction返回的类型，给我们使用的，通常将其存在到外部存储系统，比如Redis
          二元组(provinceId, orderAmt)
     */
    // 此函数针对每批次中各个Key对应的每条数据更新的函数
    val spec = StateSpec.function[Int, Double, Double, (Int, Double)](
      // (KeyType, Option[ValueType], State[StateType]) => MappedType
      (provinceId: Int, orderAmt: Option[Double], state: State[Double]) => {
        // a. 获取当前订单销售额
        val currentOrderAmt = orderAmt.getOrElse(0.0)
        // b. 获取以前Key的状态（订单销售额）
        val previousOrderAmt = if (state.exists()) state.get() else 0.0
        // c. 获取最新Key的状态
        val newOrderAmt = currentOrderAmt + previousOrderAmt
        // d. 更新最新状态
        state.update(newOrderAmt)
        // e. 返回最新状态
        (provinceId, newOrderAmt)
      }
    )
    val orderAmtTotal = orderDStream.mapWithState[Double, (Int, Double)](spec)

    // TODO: 4、将每批次结果RDD进行输出，调用foreachRDD函数
    orderAmtTotal.foreachRDD { (rdd, time) =>
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
      CKPT_DIR, // get表示检查点目录存在，就从检查目录中的数据恢复，没有的话调用下面函数创建
      () => {
        // 设置应用的相关属性配置
        val conf: SparkConf = new SparkConf()
          .setMaster("local[3]")
          .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
          .set("spark.streaming.kafka.maxRatePerPartition", "10000")
          // TODO: 在此设置Streaming 应用优雅的停止
          .set("spark.streaming.stopGracefullyOnShutdown", "true")
        // 设置Streaming应用批处理时间间隔BatchInterval为5秒
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

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

    // TODO: 核心点，当Streaming应用启动以后，监控HDFS目录中停止标记文件是否存在， 如果存在，优雅的停止应用
    var isStreamingStop = false
    val checkStopFile = "/datas/spark/streaming/amt-start-stop.txt"
    // 设置时间间隔，每隔10s钟检测：停止标记文件是否存在
    val checkInterval = 10000 // 单位是毫秒
    // 通过While循环，不断检测是否存在
    while (!isStreamingStop) {
      // 首先Streaming应用运行的时候 让等待10s，再去检查
      context.awaitTerminationOrTimeout(checkInterval)
      // 检查文件是否存在
      if (!isStreamingStop && isStopFileExists(checkStopFile, context.sparkContext.hadoopConfiguration)) {
        isStreamingStop = true
        // 优雅的停止
        context.stop(stopSparkContext = true, stopGracefully = true)
      }
    }
  }


  /**
    * 检测文件是否存在
    *
    * @param checkFile 检测的文件名称
    * @return
    */
  def isStopFileExists(checkFile: String, hadoopConf: Configuration): Boolean = {
    // a. 获取HDFs上PATH
    val checkPath: Path = new Path(checkFile)
    // b. 获取HDFS文件系统
    val hdfs: FileSystem = checkPath.getFileSystem(hadoopConf)
    // c. 判断文件是否存在
    hdfs.exists(checkPath)
  }

}
