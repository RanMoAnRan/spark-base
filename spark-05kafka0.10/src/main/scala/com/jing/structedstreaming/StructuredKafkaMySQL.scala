package com.jing.structedstreaming

import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 基于Structured Streaming 模块读取Kafka Topic取数据，进行词频统计WordCount，将结果保存到数据库
  * Structured Streaming  = SparkSQL + SparkStreaming
  */
object StructuredKafkaMySQL {

  def main(args: Array[String]): Unit = {

    // TODO： 1. 构建SparkSession实例对象，传递sparkConf参数
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._


    // TODO: 2. 使用SparkSession从TCP Socket读取流式数据
    // 文档：http://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
    val kafkaStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        "hadoop01:9092,hadoop02:9092,hadoop03:9092")
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


    // TODO: 4. 将计算的结果保存到数据库
    val query: StreamingQuery = wordcountsStreamDF
      .coalesce(1)
      .writeStream
      // 输出模式为Complete类似updateStateByKey函数功能，Update类似mapWithState
      .outputMode(OutputMode.Complete())
      //该foreach操作允许对输出数据计算任意操作。从Spark 2.1开始，这仅适用于Scala和Java。要使用它，
      // 您必须实现接口ForeachWriter ，该接口具有在触发器之后生成作为输出生成的行序列时被调用的方法
      .foreach(new MySQLForeachWriter())
      // 设置检查点目录, 自动将相关数据保存检查点目录；当再次启动应用，自动从检查目录恢复
      .option("checkpointLocation", "datas/streaming/kafka-mysql-10002")
      .start() // 流式dataFrame，需要启动

    // 查询器一直等待流式应用结束
    query.awaitTermination()
  }

}
