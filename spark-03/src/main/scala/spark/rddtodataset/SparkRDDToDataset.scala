package spark.rddtodataset

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 演示：将RDD转换为Dataset或者DataFrame
  */
object SparkRDDToDataset {

  def main(args: Array[String]): Unit = {

    // 1、构建SparkSession实例对象和SparkContext实例对象
    val (spark, sc) = {
      // a. 构建SparkSession实例对象
      val session = SparkSession.builder()
        .appName("SparkRDDToDataset")
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
    import org.apache.spark.sql.functions._


    // 2、使用SparkContext读取文件数据（电影评分数据）
    val rawRatingsRDD: RDD[String] = sc.textFile("datas/ml-100k/u.data", minPartitions = 4)


    // TODO: 3、方式一：反射推断
    /*
      a. RDD[CaseClass]
      b. rdd.toDF(), rdd.toDS()
     */
    // 转换RDD类型
    val ratingsRDD: RDD[MLRating] = rawRatingsRDD.mapPartitions { iter =>
      iter.map { item =>
        // 按照制表符分割
        val Array(userId, itemId, rating, timestamp) = item.trim.split("\\t")
        // 返回对象
        MLRating(userId.toInt, itemId.toInt, rating.toDouble, timestamp.toLong)
      }
    }

    // 隐式转换为DataFrame或Dataset
    val ratingsDF: DataFrame = ratingsRDD.toDF()
    ratingsDF.printSchema()
    ratingsDF.show(10, truncate = false)

    val ratingsDS: Dataset[MLRating] = ratingsRDD.toDS()
    ratingsDS.printSchema()
    ratingsDS.show(10, truncate = false)

    println("=======================================================")

    // TODO: 方式二：自定义Schema
    /*
      a. RDD[Row]
      b. schema
      c. spark.createDataFrame(rowsRDD, schema)
     */
    // 转换RDD类型为Row
    val rowRatingsRDD: RDD[Row] = rawRatingsRDD.mapPartitions { iter =>
      iter.map { item =>
        // 按照制表符分割
        val Array(userId, itemId, rating, timestamp) = item.trim.split("\\t")
        // 返回对象,调用的事Row中apply方式构建对象
        Row(userId.toInt, itemId.toInt, rating.toDouble, timestamp.toLong)
      }
    }
    // 自定义Schema信息


    val ratingSchema: StructType = StructType(
      Array(
        StructField("userId", IntegerType, nullable = true),
        StructField("itemId", IntegerType, nullable = true),
        StructField("rating", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      )
    )
    // 调用函数转换
    val mlRatingsDF: DataFrame = spark.createDataFrame(rowRatingsRDD, ratingSchema)
    mlRatingsDF.printSchema()
    mlRatingsDF.show(10, truncate = false)

    // 转换为Dataset
    val mlRatingsDS: Dataset[MLRating] = mlRatingsDF.as[MLRating]
    mlRatingsDS.printSchema()
    mlRatingsDS.show(10, truncate = false)


    // 应用结束关关闭资源
    sc.stop()
    // spark.stop()
  }

}
