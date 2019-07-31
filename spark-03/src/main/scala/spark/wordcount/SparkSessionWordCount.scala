package spark.wordcount

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 在Spark 2.x中提供应用程序的新入口SparkSession，使用此类实现词频统计WordCount
  */
object SparkSessionWordCount {

  def main(args: Array[String]): Unit = {

    // 使用建造者（build pattern）设计模式创建实例对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("SparkSessionWordCount")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "hdfs://hadoop01:8020/spark/eventLogs/")
      .config("spark.eventLog.compress", "true")
      // 调用函数获取SparkSession实例对象，线程安全的，如果存在就获取，不存在就创建，底层创建SparkContext实例对象
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // 读取数据, Dataset = RDD + Schema(约束，字段名称和字段类型）
    val inputDS: Dataset[String] = spark.read.textFile("/datas/wordcount.data")


    inputDS.printSchema()
    println(inputDS.schema)
    inputDS.show(10)

    // 处理数据, DataFrame = Dataset[Row]
    val wordcountsDF: DataFrame = inputDS
      .flatMap(line => line.trim.split("\\s+").filter(word => word.trim.length > 0))
      .groupBy("value").count()

    wordcountsDF.printSchema()
    wordcountsDF.show(10)

    Thread.sleep(1000000)

    // 应用结束，关闭资源
    spark.stop()
  }

}
