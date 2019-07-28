package spark.parquet

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * SparkSQL读取parquet格式数据
  */
object SparkSQLParquet {

	def main(args: Array[String]): Unit = {

		// 1、构建SparkSession实例对象和SparkContext实例对象
		val (spark, sc) = {
			// a. 构建SparkSession实例对象
			val session = SparkSession.builder()
				.appName("SparkSQLParquet")
				.master("local[4]")
				.getOrCreate()
			// b. 获取SparkContext实例对象
			val context = session.sparkContext
			// c. 设置日志级别
			context.setLogLevel("WARN")
			// d. 返回即可
			(session, context)
		}



		// 2、通过指定format格式读取parquet格式数据
		val usersDF: DataFrame = spark.read.parquet("datas/resources/users.parquet")
		usersDF.printSchema()
		usersDF.show(10, truncate = true)

		println("========================================")

		val loadDF: DataFrame = spark.read.load("datas/resources/users.parquet")
		loadDF.printSchema()
		loadDF.show(10, truncate = false)


		// 3、应用结束，关闭资源
		sc.stop()
	}

}
