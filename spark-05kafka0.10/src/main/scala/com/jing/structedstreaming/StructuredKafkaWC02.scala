package com.jing.structedstreaming

import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 基于Structured Streaming 模块读取Kafka Topic取数据，进行词频统计WordCount，将结果打印到控制台
  * Structured Streaming  = SparkSQL + SparkStreaming
  */
object StructuredKafkaWC02 {

  def main(args: Array[String]): Unit = {

    // TODO： 1. 构建SparkSession实例对象，传递sparkConf参数
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._


    // TODO: 2. 使用SparkSession从kafka读取流式数据
    // 文档：http://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
    val kafkaStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
      .option("subscribe", "orderTopic")
      .load()

    // TODO: 3. 针对获取流式DStream进行词频统计
    val wordcountsStreamDF: DataFrame = kafkaStreamDF
      // 获取topic每条数据的value值，转换为String类型
      .selectExpr("CAST(value AS STRING)")
      // 将DataFrame转换为Dataset操作，Dataset是类型安全，强类型
      .as[String]
      // 过滤无效数据
      .filter(trim($"value").isNotNull)
      // 将每行数据进行分割单词
      .flatMap(line => line.trim.split("\\s+").filter(word => word.length > 0))
      // 按照单词分组，使用count函数
      .groupBy($"value").count()


    // TODO: 4. 将计算的结果输出，打印到控制台
    val query: StreamingQuery = wordcountsStreamDF.writeStream
      // 输出模式为Complete类似updateStateByKey函数功能，Update类似mapWithState
      .outputMode(OutputMode.Complete())
      .format("console")
      // TODO设置触发器
      .trigger(ProcessingTime.create("2 seconds"))
      // 设置检查点目录, 自动将相关数据保存检查点目录；当再次启动应用，自动从检查目录恢复
      //但是（format("console")模式下）不支持从检查点恢复
      //.option("checkpointLocation", "datas/streaming/kafka-xx002")
      .start() // 流式dataFrame，需要启动

    // 查询器一直等待流式应用结束
    query.awaitTermination()
  }

}
