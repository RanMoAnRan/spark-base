package com.jing.structedstreaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 基于Structured Streaming 模块读取TCP Socket读取数据，进行词频统计WordCount，将结果打印到控制台
  * Structured Streaming  = SparkSQL + SparkStreaming
  */
object StructuredWordCount01 {

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
    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "hadoop01") //linux命令 nc -lk 9999 开启端口
      .option("port", 9999)
      .load()

    // TODO: 3. 针对获取流式DStream进行词频统计
    val wordcountsStreamDF: DataFrame = inputStreamDF
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
      .outputMode(OutputMode.Update())
      .format("console")
      .start() // 流式dataFrame，需要启动

    // 查询器一直等待流式应用结束
    query.awaitTermination()
  }

}
