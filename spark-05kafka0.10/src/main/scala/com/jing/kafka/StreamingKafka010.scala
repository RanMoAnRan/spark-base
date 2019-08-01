package com.jing.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Streaming通过Kafka New Consumer消费者APi获取数据
  */
object StreamingKafka010 {

  def main(args: Array[String]): Unit = {

    // TODO: 1. 构建StreamingContext流式上下文实例对象
    val ssc: StreamingContext = {
      val sparkConf = new SparkConf()
        .setMaster("local[4]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .set("spark.streaming.kafka.maxRatePerPartition", "100000")
      // ef getActiveOrCreate(creatingFunc: () => StreamingContext): StreamingContext
      val context = StreamingContext.getActiveOrCreate(
        () => {
          new StreamingContext(sparkConf, Seconds(5)) // 设置BatchInterval
        }
      )
      context.sparkContext.setLogLevel("WARN")
      // 返回
      context
    }


    // TODO: 2. 读取Kafka Topic中数据
    // 文档：http://spark.apache.org/docs/2.2.0/streaming-kafka-0-10-integration.html#creating-a-direct-stream
    /*
      def createDirectStream[K, V](
        ssc: StreamingContext,
        locationStrategy: LocationStrategy,
        consumerStrategy: ConsumerStrategy[K, V]
      ): InputDStream[ConsumerRecord[K, V]]
     */
    // 位置策略
    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
    /*
      def Subscribe[K, V](
        topics: ju.Collection[jl.String],
        kafkaParams: ju.Map[String, Object]
       ): ConsumerStrategy[K, V]
     */
    // 读取哪些Topic数据
    val topics = Array("orderTopic")
    // 消费Kafka 数据配置参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      //latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)

    )
    // 消费数据策略
    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(
      topics, kafkaParams
    )
    // 采用新消费者API获取数据，类似于Direct方式
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)


    // 直接输出Value的值
    kafkaDStream.foreachRDD { (rdd, time) =>
      println(s"================== $time =================")
      if (!rdd.isEmpty()) {
        //打印offset的信息
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println(s"offsetRanges=${offsetRanges}")
        rdd.foreachPartition { iter =>
          iter.foreach { record =>
            println(s"${record.topic()}-${record.partition()}:${record.offset()} = ${record.value()}")
          }
        }
        println("===================================")
        // 等输出操作完成后提交offset
         kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
