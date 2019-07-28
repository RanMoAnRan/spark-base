package spark.csv

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * 读取CSV格式数据
  */
object SparkSQLCsv {

  def main(args: Array[String]): Unit = {

    // 1、构建SparkSession实例对象和SparkContext实例对象
    val (spark, sc) = {
      // a. 构建SparkSession实例对象
      val session = SparkSession.builder()
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

    // 2、读取CSV格式数据，当文件的首行是列的名称
    val mlDF: DataFrame = spark.read
      .option("sep", "\\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("datas/ml-100k/u.dat")


    mlDF.printSchema()
    mlDF.show(10, truncate = false)


    println("===================================================")

    // TODO: 当使用CSV读取数据的时候，如果首行不是列名称，自定义Schema信息
    val ratingSchema = StructType(
      StructField("userId", IntegerType, nullable = true) ::
        StructField("movieId", IntegerType, nullable = true) ::
        StructField("rating", DoubleType, nullable = true) ::
        StructField("timestamp", LongType, nullable = true) :: Nil
    )


    val ratingsDF: DataFrame = spark.read
      .schema(ratingSchema)
      .option("sep", "\\t")
      .option("header", "false")
      .option("inferSchema", "false")
      .csv("datas/ml-100k/u.data")

    ratingsDF.printSchema()
    ratingsDF.show(10, truncate = false)



    // 3、应用结束，关闭资源
    sc.stop()
  }

}
