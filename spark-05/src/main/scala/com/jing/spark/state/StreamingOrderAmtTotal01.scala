package com.jing.spark.state

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时统计各个省份销售订单额，从Kafka Topic（`orderTopic`）中读取数据，
  * 每个订单数据中各个字段信息使用逗号隔开（orderId,provinceId,orderPrice）
  * 状态统计updateStateByKey API
  */
object StreamingOrderAmtTotal01 {
  def main(args: Array[String]): Unit = {

    //构建StreamingContext流式应用上下文实例对象
    val ssc: StreamingContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .set("spark.streaming.kafka.maxRatePerPartition", "10000")

      // 设置Streaming应用批处理时间间隔BatchInterval为5秒
      val streamingContext = new StreamingContext(sparkConf, Seconds(5))
      streamingContext.sparkContext.setLogLevel("WARN")
      //设置检查点文件目录
      streamingContext.checkpoint("datas/streaming/state/order-amt-000001")
      streamingContext
    }

    // Kafka 相关配置参数，指的是消费者的相关配置参数
    val kafkaParams: Map[String, String] = Map(
      "bootstrap.servers" -> "hadoop01:9092,hadoop01:9092,hadoop01:9092",
      "auto.offset.reset" -> "largest"
    )
    // 指定从哪些topic中读取数据，可以设置多个topic
    val topics: Set[String] = Set("orderTopic")
    // 从Kafka Topic中读取数据，采用Direct方式
    val kafkaDStream: DStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics
    )

    //对流式的数据进行处理分析，调用DStream中函数
    val orderDStream: DStream[(Int, Double)] = kafkaDStream.transform(rdd => {
      // 数据格式：orderId,provinceId,orderPrice
      rdd.filter(msg => msg._2 != null && msg._2.trim.split(",").length >= 3)
        .mapPartitions { iter =>
          iter.map {
            case (_, value) =>
              val Array(_, provinceId, orderPrice) = value.trim.split(",")
              // 返回二元组(省份ID, 订单金额）
              (provinceId.toInt, orderPrice.toDouble)
          }
        }
        .reduceByKey(_ + _)
    })

    /*
		  依据每批次中Key来统计和更新状态信息（当前批次中Key的状态和以前的状态）
		  def updateStateByKey[S: ClassTag](
			  updateFunc: (Seq[V], Option[S]) => Option[S]
			): DStream[(K, S)]

		  状态更新函数：updateFunc: (Seq[V], Option[S]) => Option[S]
		  	-a. 参数 ->   (Seq[V], Option[S])
		  		Seq[V]:
		  			集合，V表示DStream中Value的类型，针对应用来说就是各个订单的销售额orderPrice
		  		Option[S]：  当前Key以前的状态
		  			S 表示Key的转态类型，针对应用就是省份总的销售订单额
		  			使用Option原因在于，可能当前key在以前没有状态，使用None表示，有状态使用Some
		  	-b. 返回类型
		 		Option[S] ：表示当前Key的最新状态，封装在Option中，
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
        //设置分区数为1
        rdd.coalesce(1)
          .foreachPartition { datas => datas.foreach(println) }
      }
    }


    // TODO: 5、针对流式应用需要启动程序
    ssc.start()
    // 对于流式应用，通常情况下只要启动一直运行处理流式数据，除非认为关闭或者程序异常才终止，等待程序终止
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}

