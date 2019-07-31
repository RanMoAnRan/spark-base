package spark.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * SparkSQL中如何自定义函数UDF：一对一，此函数输入一个值，返回一个值。
  */
object SparkSqlUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      //设置shuffle分区数
      .config("spark.sql.shuffle.partitions", "4")
      // 表示与Hive集成
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // 将匿名函数赋值给一个变量
    var tolow = (str: String) => {
      str.trim.toLowerCase
    }

    // 第一种方式：定义UDF函数，在SQL中使用
    spark.udf.register(
      "to_lower" //函数名
      , tolow)

    spark.sql(
      """
        			  |SELECT ename, to_lower(ename) AS name FROM db_hive.emp
      			""".stripMargin)
      .show()

    println("========================================================")

    // 第二种方式：定义UDF函数，在DSL中使用
    import org.apache.spark.sql.functions._
    val transf_lower: UserDefinedFunction = udf(tolow)

    spark.read
      .table("db_hive.emp")
      .select($"ename", transf_lower($"ename").as("name"))
      .show()


    Thread.sleep(100000)

    // 应用结束，关闭资源
    spark.stop()

  }

}
