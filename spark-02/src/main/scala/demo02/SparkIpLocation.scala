package demo02

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过日志信息（运行商或者网站自己生成）和城市ip段信息来判断用户的ip段，统计热点经纬度
  *		a. 加载IP地址信息库，获取起始IP地址和结束IP地址Long类型值及对应经度和维度
  *		b. 读取日志数据，提取IP地址，将其转换为Long类型的值
  *		c. 依据Ip地址Long类型值获取对应经度和维度 - 二分查找
  *		d. 按照经度和维分组聚合统计出现的次数，并将结果保存到MySQL数据库中
  */
object SparkIpLocation {

  /**
    * 将IPv4格式值转换为Long类型值
    *
    * @param ip
    * IPv4格式的值
    * @return
    */
  def ipToLong(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    // 返回
    ipNum
  }

  /**
    * 依据IP地址Long类型的值到IP地址信息库数组中查找，采用二分查找算法
    *
    * @param ipLong      IP地址Long类型的值
    * @param ipInfoArray IP地址信息库数组
    * @return 数组下标索引，如果是-1表示未查到
    */
  def binarySearch(ipLong: Long, ipInfoArray: Array[(Long, Long, String, String)]): Int = {
    // 定义查找索引下标
    var startIndex = 0
    var endIndex = ipInfoArray.length - 1

    // While 循环判断
    while (startIndex <= endIndex) {
      // 依据起始索引和结束索引计算出中间索引值
      val middleIndex = startIndex + (endIndex - startIndex) / 2

      // 依据中间索引获取数组中对应的值
      val (startIp, endIp, _, _) = ipInfoArray(middleIndex) // 拆箱操作

      // 将获得起始IP和结束IP与查找IP地址进行比较
      if (ipLong >= startIp && ipLong <= endIp) {
        return middleIndex
      }

      // 当ipLong 小于起始IP地址，继续从数组左边查找
      if (ipLong < startIp) {
        endIndex = middleIndex - 1
      }

      // 当ipLong 大于结束IP地址，继续从数组右边查找
      if (ipLong > endIp) {
        startIndex = middleIndex + 1
      }
    }
    // 当数组中未查找到，返回-1
    -1
  }


  /**
    * 将RDD中分区的数据保存到MySQL数据库的表中
    *
    * @param iter 迭代器
    */
  def saveToMySQL(iter: Iterator[((String, String), Int)]): Unit = {

    // i. 加载驱动类
    Class.forName("com.mysql.jdbc.Driver")
    // 声明变量
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    // 插入SQL语句,
    //val sqlStr = "INSERT INTO test.tb_iplocation(longitude, latitude, total_count) VALUES (?, ?, ?)"

    // TODO: 插入更新，当主键存在时更新值，不存在插入
    val sqlStr = "INSERT INTO tb_iplocation (longitude, latitude, total_count) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE total_count=VALUES(total_count)";

    try {
      // ii、获取连接
      conn = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/test", "root", "root"
      )
      pstmt = conn.prepareStatement(sqlStr)

      // 设置事务，改为手动提交
      val defaultCommit = conn.getAutoCommit // 先获取数据自身事务
      conn.setAutoCommit(false)

      // iii. 插入数据到表中，批量插入
      iter.foreach { case ((longitude, latitude), cnt) =>
        pstmt.setString(1, longitude)
        pstmt.setString(2, latitude)
        pstmt.setLong(3, cnt)

        // 加入批次中
        pstmt.addBatch()
      }
      // 批量插入
      pstmt.executeBatch()

      conn.commit()
      conn.setAutoCommit(defaultCommit) // 设置数据库原来事务
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != pstmt) pstmt.close()
      if (null != conn) conn.close()
    }
  }

  def main(args: Array[String]): Unit = {

    // TODO: 1、构建SparkContext实例对象，读取数据和调度Job执行
    val sc: SparkContext = {
      val sparkConf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("SparkPvUvReferTopKey")
      // 调用getOrCreate函数创建实例对象，当SparkContext对象存在时获取，否则创建新的
      val context: SparkContext = SparkContext.getOrCreate(sparkConf)
      context.setLogLevel("WARN")

      context
    }


    // TODO: 2、加载IP地址信息库，获取起始IP地址和结束IP地址Long类型值及对应经度和维度
    val ipInfoRDD: RDD[(Long, Long, String, String)] = sc
      // 读取本地文件系统中数据
      .textFile("datas/ips/ip.txt", minPartitions = 2)
      .filter(line => null != line && line.trim.split("\\|").length == 15)
      // 提取字段信息
      .mapPartitions { datas =>
      datas.map { data =>
        // 按照分隔符切割
        val arr = data.trim.split("\\|")
        // 以四元组形式返回(起始IP地址Long值   结束IP地址Long值   经度  维度)
        (arr(2).toLong, arr(3).toLong, arr(arr.length - 1), arr(arr.length - 2))
      }
    }
    // 如何将RDD数据转存到数组Array, 此数组中IP地址有序的，升序排序
    val ipInfoArray: Array[(Long, Long, String, String)] = ipInfoRDD.collect()
    // 通过Spark 中广播变量将数组广播到Executor中，被Task使用
    val ipInfoBroadcast: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(ipInfoArray)


    // TODO: 3、读取日志数据，提取IP地址，将其转换为Long类型的值
    val ipLocationRDD: RDD[((String, String), Int)] = sc
      .textFile("datas/ips/20090121000132.394251.http.format", minPartitions = 4)
      .filter(log => null != log && log.trim.split("\\|").length >= 2)
      .mapPartitions { iter =>
        iter.map { log =>
          // i. 获取IP地址
          val ipValue = log.trim.split("\\|")(1)
          // ii. 将IP地址转换为Long类型值
          val ipLong: Long = ipToLong(ipValue)
          // iii. 依据Ip地址Long类型值获取对应经度和维度
          val index = binarySearch(ipLong, ipInfoBroadcast.value)
          // 返回
          index
        }
      }
      // 过滤掉未查找到IP地址数据
      .filter(index => index != -1)
      // 获取对应经度和维度信息
      .mapPartitions { iter =>
      iter.map { index =>
        // 依据数组下标index，到数组中获取对应的经度和维度
        val (_, _, longitude, latitude) = ipInfoBroadcast.value(index)
        // 以二元组形式返回
        ((longitude, latitude), 1)
      }
    }
      // 按照经度和维度分组聚合统计
      .reduceByKey(_ + _)

    // ipLocationRDD.foreach(println)

    // TODO: 结果保存到MySQL数据库中
    ipLocationRDD
      // 降低分区数
      .coalesce(1)
      .foreachPartition { iter =>
        // val xx: Iterator[((String, String), Int)] = iter
        saveToMySQL(iter)
      }


    Thread.sleep(1000000)

    // 当应用结束，关闭SparkContext
    if (!sc.isStopped) sc.stop()

  }

}
