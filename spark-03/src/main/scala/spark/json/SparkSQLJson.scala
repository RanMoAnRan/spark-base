package spark.json

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * SparkSQL读取JSON格式的数据
  */
object SparkSQLJson {

  def main(args: Array[String]): Unit = {


    // 1、构建SparkSession实例对象和SparkContext实例对象
    val (spark, sc) = {
      // a. 构建SparkSession实例对象
      val session: SparkSession = SparkSession.builder()
        .appName(this.getClass.getSimpleName)
        .master("local[4]")
        .getOrCreate()
      // b. 获取SparkContext实例对象
      val context = session.sparkContext
      // c. 设置日志级别
      context.setLogLevel("WARN")
      // d. 返回即可
      (session, context)
    }
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._


    // 2、采用SparkSQL中对JSON格式读取数据（此方式一般用于json格式的文件数据）
    val jsonDF: DataFrame = spark.read
      .json("datas/json/2015-03-01-11.json.gz")
      .select($"id", $"type", $"public", $"created_at")

    jsonDF.printSchema()
    jsonDF.show(10, truncate = true)

    println("====================================")

    // TODO: 从Spark 2.x中提供对JSON格式数据处理的数据(此方式常用于处理实时数据)
    val gitDS: Dataset[String] = spark.read.textFile("datas/json/2015-03-01-11.json.gz")

    /*
    root
       |-- value: string (nullable = true)
     */
    // gitDS.printSchema()
    gitDS.show(10,truncate = false)

    // 从JSON格式字段中提取数据  $"id", $"type", $"public", $"created_at"
    import org.apache.spark.sql.functions._

    // def get_json_object(e: Column, path: String): Column
    gitDS
      .select(
        get_json_object($"value", "$.id").as("id"),
        get_json_object($"value", "$.type").as("type"),
        get_json_object($"value", "$.public").as("public"),
        get_json_object($"value", "$.created_at").as("created_at")
      )
      .show(10)


    // 3、应用结束，关闭资源
    sc.stop()
  }

}
