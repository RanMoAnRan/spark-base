package spark.jdbc

import java.util.Properties

import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * SparkSQL从MySQL表中直接读取数据，总共有三种方式
  */
object SparkSQLMySQL {

  def main(args: Array[String]): Unit = {

    // 1、构建SparkSession实例对象和SparkContext实例对象
    val (spark, sc) = {
      // a. 构建SparkSession实例对象
      val session = SparkSession.builder()
        .appName("SparkSQLMySQL")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
      // b. 获取SparkContext实例对象
      val context = session.sparkContext
      // c. 设置日志级别
      context.setLogLevel("WARN")
      // d. 返回即可
      (session, context)
    }


    // TODO： 2、从MySQL表中读取销售订单表的数据db_orders.so
    val url = "jdbc:mysql://localhost:3306/db_orders"
    val table: String = "db_orders.so"
    val props: Properties = new Properties()
    props.put("user", "root")
    props.put("password", "root")
    props.put("driver", "com.mysql.jdbc.Driver")

    /*
      方式一：单分区
      def jdbc(url: String, table: String, properties: Properties): DataFrame
     */
    val soDF: DataFrame = spark.read.jdbc(url, table, props)

    println(s"partitions = ${soDF.rdd.getNumPartitions}, count = ${soDF.count()}")
    soDF.printSchema()
    soDF.show(10, truncate = false)


    /**
      * 方式二：多分区模式(会导致数据倾斜)
      * def jdbc(
      * url: String,
      * table: String,
      * columnName: String,  // 按照表中哪个字段列名分区
      * lowerBound: Long,  // 下限值
      * upperBound: Long,  // 上限值
      * numPartitions: Int, // 分区数
      * connectionProperties: Properties
      * ): DataFrame
      */
    val soDF2: DataFrame = spark.read.jdbc(
      url, table,
      "order_amt", 10L, 200L, 5,
      props
    )
    // 打印每个分区的额数据
    soDF2.foreachPartition { iter =>
      println(s"p-${TaskContext.getPartitionId()}, count = ${iter.size}")
    }


    println("=====================================================")
    /*
    方式三：
      def jdbc(
        url: String,
        table: String,
        // 谓词下压，过滤条件，通过设置条件决定每个分区的数据
        predicates: Array[String],
        connectionProperties: Properties
      ): DataFrame
     */
    //
    val predicates: Array[String] = Array(
      "order_amt >=20 AND order_amt < 30", "order_amt >= 30 AND order_amt < 45",
      "order_amt >= 45 AND order_amt < 80", "order_amt >= 80 AND order_amt <= 200"
    )
    //
    val soDF3 = spark.read.jdbc(url, table, predicates, props)
    // 打印每个分区的额数据
    soDF3.foreachPartition { iter =>
      println(s"p-${TaskContext.getPartitionId()}, count = ${iter.size}")
    }

    Thread.sleep(100000)

    // 3、应用结束，关闭资源
    sc.stop()

  }

}
