package demo01

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对网站点击流日志分析：
  * a. PV    -  第7个字段  url
  * 	网站浏览量，URL不为NULL
  *b. UV     - 第1个字段  ip
  * 	唯一访客数，企业中采集数据的时候，专门定制guid，用于统计唯一用户，此处为方便使用IP统计，cookie或者session。
  *c. Refer TopKey   -  第11个字段  refer
  * 	外链统计，最多5个Refer
  */
object SparkPvUvReferTopKey {

	def main(args: Array[String]): Unit = {

		// TODO: 1、构建SparkContext实例对象，读取数据和调度Job执行
		val sparkConf = new SparkConf()
	    	.setMaster("local[4]")
	    	.setAppName("SparkPvUvReferTopKey")
		// 调用getOrCreate函数创建实例对象，当SparkContext对象存在时获取，否则创建新的
		val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
		sc.setLogLevel("WARN")


		// TODO: 2、读取网站点击流日志数据，从本地文件系统读取LocalFS
		val accessLogsRDD: RDD[String] = sc.textFile("datas/logs/access.log", minPartitions = 4)


		val filterRDD: RDD[(String, String, String)] = accessLogsRDD
			// 过滤不合格的数据：为null或者分隔长度小于11
	    	.filter(log => null != log && log.trim.split("\\s").length >= 11)
			//提取字段
	    	.mapPartitions{iter =>
				// 对每个分区数据操作
				iter.map{ log =>
					// 按照空格分隔每条日志数据
					val arr: Array[String] = log.trim.split("\\s")
					// 以三元组形式返回(ip, url. refer)
					(arr(0), arr(6), arr(10))
				}
			}

		// 由于后续的需求使用RDD多次，所以缓存
		filterRDD.persist(StorageLevel.MEMORY_AND_DISK)
		// 触犯缓存
		println(s"count = ${filterRDD.count()}")


		// TODO： 3.1、PV 统计
		val totalPV: Long = filterRDD
			// 提取字段
	    	.mapPartitions{ iter => iter.map(tuple => tuple._2)}
			// 过滤url为空
	    	.filter{ url => null != url && url.trim.length > 0}
			// 统计出现次数
	    	.count()
		println(s"PV = $totalPV")


		// TODO: 3.2、 UV 统计
		val totalUv: Long = filterRDD
			.mapPartitions{ iter => iter.map(tuple => tuple._1)}
			.filter{ url => null != url && url.trim.length > 0}
			// 去重
	    	.distinct()
	    	.count()
		println(s"UV = $totalUv")


		// TODO: 3.3、 TopKey Refer
		val top10ReferArray: Array[(String, Int)] = filterRDD
			.mapPartitions{ iter => iter.map(tuple => (tuple._3, 1)) }
	    	.filter{ case(refer, _) => null != refer && refer.trim.length > 0}
			// 分组统计，使reduceByKey函数
	    	.reduceByKey((a, b) => a + b)
			// 对统计次数降序排序
	    	.sortBy(tuple => tuple._2, ascending = false)
	    	.take(10)
		top10ReferArray.foreach(x => println(x))

		// 释放缓存资源
		filterRDD.unpersist()


		Thread.sleep(1000000)


		// 当应用结束，关闭SparkContext
		if(!sc.isStopped) sc.stop()
	}

}
