package com.jing.structedstreaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * 将结构化流处理分析的StreamDataFrame保存到MySQL表中，针对每个分区操作
  */
class MySQLForeachWriter extends ForeachWriter[Row]{

	var conn: Connection = _
	var pstmt: PreparedStatement = _
	var defaultCommit: Boolean = _
	val sqlStr = "INSERT INTO sstb_wordcount (word, count) VALUES(?, ?) ON DUPLICATE KEY UPDATE count=VALUES(count)" ;

	// open connection
	override def open(partitionId: Long, version: Long): Boolean = {
		// i. 加载驱动类
		Class.forName("com.mysql.jdbc.Driver")
		// ii、获取连接
		conn = DriverManager.getConnection(
			"jdbc:mysql://localhost:3306/test", "root", "root"
		)
		pstmt = conn.prepareStatement(sqlStr)

		// 设置事务，改为手动提交
		defaultCommit = conn.getAutoCommit  // 先获取数据自身事务
		conn.setAutoCommit(false)

		// 直接返回
		true
	}

	// write data to connection, 针对每条数据操作的
	override def process(row: Row): Unit = {
		// 设置每个字段的值
		pstmt.setString(1, row.getAs[String]("value"))
		pstmt.setLong(2, row.getAs[Long]("count"))

		// 加入批次中
		pstmt.addBatch()
	}

	// close the connection
	override def close(errorOrNull: Throwable): Unit = {
		// 批量插入
		pstmt.executeBatch()

		// 手动提交事务，当前分区的数据批量插入
		conn.commit()
		conn.setAutoCommit(defaultCommit) // 设置数据库原来事务

		// 关闭资源
		if(null != pstmt) pstmt.close()
		if(null != conn) conn.close()
	}
}
