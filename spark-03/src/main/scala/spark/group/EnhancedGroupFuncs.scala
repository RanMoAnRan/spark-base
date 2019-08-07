package spark.group

import org.apache.spark.sql.{DataFrame, SparkSession}

case class MemberOrderInfo(area: String, memberType: String, product: String, price: Int)


/**
  * 分组函数：grouping sets、group by、rollup和cube区别
  * group by：主要用来对查询的结果进行分组，相同组合的分组条件在结果集中只显
  * 示一行记录。可以添加聚合函数。
  * grouping sets：对分组集中指定的组表达式的每个子集执行group by， group by
  * A,B grouping sets(A,B) 就等价于 group by A union group by B ，其中A和
  * B也可以是一个集合，比如group by A,B,C grouping sets((A,B),(A,C))。
  * rollup：在指定表达式的每个层次级别创建分组集。 group by A,B,C with
  * rollup 首先会对 (A、B、C) 进行group by，然后对 (A、B) 进行group by，然后是
  * (A) 进行group by，最后对全表进行group by操作。
  * cube：为指定表达式集的 每个可能组合创建分组集 。 group by A,B,C with cube
  * 首先会对 (A、B、C) 进行group by，然后依次是 (A、B)，(A、C)，(A)，(B、C)，
  * (B)，( C) ，最后对全表进行group by操作。
  */
object EnhancedGroupFuncs {

  def main(args: Array[String]): Unit = {

    // 1、构建SparkSession实例对象
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "2")
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    // 导入隐式转换
    import spark.implicits._
    // 导入SparkSQL函数库
    import org.apache.spark.sql.functions._


    // 2. 构建模拟数据集
    val orders: Seq[MemberOrderInfo] = Seq(
      MemberOrderInfo("深圳", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("深圳", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("深圳", "钻石会员", "钻石会员3个月", 70),
      MemberOrderInfo("深圳", "钻石会员", "钻石会员12个月", 300),
      MemberOrderInfo("深圳", "铂金会员", "铂金会员3个月", 60),
      MemberOrderInfo("深圳", "铂金会员", "铂金会员3个月", 60),
      MemberOrderInfo("深圳", "铂金会员", "铂金会员6个月", 120),
      MemberOrderInfo("深圳", "黄金会员", "黄金会员1个月", 15),
      MemberOrderInfo("深圳", "黄金会员", "黄金会员1个月", 15),
      MemberOrderInfo("深圳", "黄金会员", "黄金会员3个月", 45),
      MemberOrderInfo("深圳", "黄金会员", "黄金会员12个月", 180),
      MemberOrderInfo("北京", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("北京", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("北京", "铂金会员", "铂金会员3个月", 60),
      MemberOrderInfo("北京", "黄金会员", "黄金会员3个月", 45),
      MemberOrderInfo("上海", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("上海", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("上海", "铂金会员", "铂金会员3个月", 60),
      MemberOrderInfo("上海", "黄金会员", "黄金会员3个月", 45)
    )
    // 直接将Seq转换为DataFrame实例对象
    val ordersDF: DataFrame = orders.toDF()
    // 下面全部使用SQL分析, 注册DataFrame为临时视图
    ordersDF.createOrReplaceTempView("view_temp_orders")


    /**
      * 需求：统计各个城市的会员数目、统计各个城市不同会员类型数目，统计各个城市不同会员购买不同产品的数目
      * 黄金会员：
      * 深圳	一个月 黄金会员： 15元
      * 深圳 三个月 黄金会员： 40元
      * 深圳	 六个月 黄金会元： 75元
      *
      * 深圳	一个月  铂金会员： 15元
      * 深圳 三个月  铂金会员： 40元
      * 深圳	 六个月  铂金会元： 75元
      */
    spark.sql(
      """
        			  |SELECT "" as area, "" as memberType, "" as product, count(1) as count FROM view_temp_orders t
        			  |UNION ALL
        			  |SELECT t.area, "" as memberType, "" as product, count(1) as count FROM view_temp_orders t GROUP BY t.area
        			  |UNION ALL
        			  |SELECT t.area, t.memberType, "" as product, count(1) as count FROM view_temp_orders t GROUP BY t.area, t.memberType
        			  |UNION ALL
        			  |SELECT t.area, t.memberType, t.product, count(1) as count FROM view_temp_orders t GROUP BY t.area, t.memberType, t.product
      			""".stripMargin)
      .orderBy($"area", $"memberType", $"product")
      .show(100, truncate = false)

    println("=======================================================================")
    spark.sql(
      """
        			  |SELECT
        			  |	t.area, t.memberType, t.product, count(1) as count
        			  |FROM
        			  |  view_temp_orders t
        			  |GROUP BY
        			  |  t.area, t.memberType, t.product GROUPING SETS(t.area, (t.area, t.memberType), (t.area, t.memberType, t.product))
      			""".stripMargin)
      .orderBy($"area", $"memberType", $"product")
      .show(100, truncate = false)


    println("=======================================================================")
    spark.sql(
      """
        			  |SELECT
        			  |	t.area, t.memberType, t.product, count(1) as count
        			  |FROM
        			  |  view_temp_orders t
        			  |GROUP BY
        			  |  t.area, t.memberType, t.product with rollup
      			""".stripMargin)
      .orderBy($"area", $"memberType", $"product")
      .show(100, truncate = false)


    println("=======================================================================")
    spark.sql(
      """
        			  |SELECT
        			  |	t.area, t.memberType, t.product, count(1) as count
        			  |FROM
        			  |  view_temp_orders t
        			  |GROUP BY
        			  |  t.area, t.memberType, t.product with cube
      			""".stripMargin)
      .orderBy($"area", $"memberType", $"product")
      .show(100, truncate = false)

    println("=======================================================================")
    // 使用DSL中高级分组函数聚合统计，金额price，使用sum函数
    ordersDF
      .groupBy($"area", $"memberType", $"product").agg(sum($"price"))
      .orderBy($"area", $"memberType", $"product")
      .show(100, truncate = false)

    println("=======================================================================")
    ordersDF.rollup($"area", $"memberType", $"product").agg(sum($"price").as("total_price"))
      .orderBy($"area", $"memberType", $"product")
      .show(100, truncate = false)

    println("=======================================================================")
    ordersDF.cube($"area", $"memberType", $"product").agg(sum($"price"))
      .orderBy($"area", $"memberType", $"product")
      .show(100, truncate = false)
  }

}
