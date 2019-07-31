package spark.hive

import org.apache.spark.sql.SparkSession

/**
  * SparkSQL集成Hive，读取表中的数据
  */
object SparkSqlHive {
  def main(args: Array[String]): Unit = {
    // 1、构建SparkSession实例对象
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      // 表示数据仓库目录, 当与Hive集成的时候，Hive表的数据存储的HDFs目录
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      // 表示与Hive集成
      .enableHiveSupport()
      .getOrCreate()


    // 读取Hive中db_hive库中的emp和dept表的数据
    spark.sql("select * from myhive.techer").show(10,truncate = false)

    Thread.sleep(100000)


    // 应用结束，关闭资源
    spark.stop()
  }
}
