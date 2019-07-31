package spark.process

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * 需求：
  * 对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
  * *
  * 数据格式：6040::1077::5::964828799
  */
object SparkSQLProcessing {

  def main(args: Array[String]): Unit = {

    // 1、构建SparkSession实例对象和SparkContext实例对象
    val (spark, sc) = {
      // a. 构建SparkSession实例对象
      val session = SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[4]")
        //如果不设置此参数， 当SparkSQL中程序产生聚合或者Join是，Shuffle的分区数据为200
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
      // b. 获取SparkContext实例对象
      val context = session.sparkContext
      // c. 设置日志级别
      context.setLogLevel("WARN")
      // d. 返回即可
      (session, context)
    }

    import spark.implicits._


    // 2、读取数据, 通过调用Dataset在中函数进行数据转换操作
    val ratingsDS: Dataset[RatingML] = spark.read
      // 读取文本文件数据，每行数据封装在value字段中
      .textFile("datas/ml-1m/ratings.dat") //  Dataset[String]
      .filter($"value".isNotNull)
      // 对每个分区数据操作，提取字段封装到CaseClass
      .mapPartitions { iter =>
      iter
        .filter(line => line.trim.split("::").length == 4)
        .map { line =>
          val Array(userId, movieId, rating, timestamp) = line.trim.split("::")
          RatingML(userId.toInt, movieId.toInt, rating.toDouble, timestamp.toLong)
        }
    }

    //使用DSL分析
    import org.apache.spark.sql.functions._
    ratingsDS
      .select($"movieId", $"rating") // 选举要使用的字段
      .groupBy($"movieId") // 按照电影Id进行分组
      // 应用聚合函数统计
      .agg(count($"movieId").as("cnt"), round(avg($"rating"), 2).as("avg_rating"))
      // 过滤评分次数大于2000
      .filter($"cnt" > 2000)
      // 先按次数降序排序，再按平均值降序排序
      .orderBy($"cnt".desc, $"avg_rating".desc)
      .limit(10)
      .show(10, truncate = false)

    println("=======================================================")



    // TODO: 使用SQL分析
    // 注册为临时视图
    ratingsDS.createOrReplaceTempView("view_tmp_ratings")
    // 编写SQL分析
    spark.sql(
      """
        			  |SELECT
        			  |  movieId, COUNT(1) AS cnt, ROUND(AVG(rating), 2) AS avg_rating
        			  |FROM
        			  |  view_tmp_ratings
        			  |GROUP BY
        			  |  movieId
        			  |HAVING
        			  |  cnt > 2000
        			  |ORDER BY
        			  |	  cnt DESC, avg_rating DESC
        			  |LIMIT
        			  |   10
      			""".stripMargin)
      .show(10, truncate = false)


    Thread.sleep(100000)

    // 3、应用结束，关闭资源
    sc.stop()
  }

}
