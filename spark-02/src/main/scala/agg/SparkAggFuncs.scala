package agg

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * RDD中聚合函数
  */
object SparkAggFuncs {

	// TODO: Spark Application应用入门，Driver Program，必须创建SparkContext实例对象
	def main(args: Array[String]): Unit = {

		// TODO: 1、构建Spark应用程序入口SparkContext上下文实例对象
		// 对Spark Application进行配置设置，比如设置应用的名称，设置应用运行的地方（--master）
		val sparkConf = new SparkConf()
	    	.setAppName(this.getClass.getSimpleName)
	    	.setMaster("local[2]") // 以本地模式运行岑石
		// 传递SparkConf对象，创建实例
		val sc: SparkContext = new SparkContext(sparkConf)


		// TODO: 2、采用并行化方式创建RDD
		val inputRDD: RDD[Int] = sc.parallelize(1 to 10, numSlices = 2)

		inputRDD.reduce((a, b) => {
			println(s"a = $a, b = $b, $a + $b = ${a + b}")
			a + b
		})

		inputRDD.fold(100)((a, b) => {
			println(s"a = $a, b = $b, $a + $b = ${a + b}")
			a + b
		})

		// TODO: 获取RDD中最大2个元素,看做是聚合操作，返回结果类型集合中，使用列表ListBuffer

		/*
			def aggregate[U: ClassTag]
			// 聚合时中间临时变量初始值
			(zeroValue: U)
			(
				// 表示对每个分区数据聚合的函数
				seqOp: (U, T) => U,
				// 表示全局聚合，对各个分区聚合结果进行聚合的函数
				combOp: (U, U) => U
			): U
		 */
		inputRDD.aggregate(new ListBuffer[Int]())(
			// seqOp: (U, T) => U
			(u: ListBuffer[Int], t: Int) => {
				// 将分区中每个元素加入到ListBuffer中
				u += t
				// 对List降序排序，获取最大的2个
				u.sortBy(- _).take(2)
			},
			// combOp: (U, U) => U
			(u1: ListBuffer[Int], u2: ListBuffer[Int]) => {
				// 将两个ListBuffer合并
				u1 ++= u2
				// 对List降序排序，获取最大的2个
				u1.sortBy(- _).take(2)
			}
		)


		inputRDD.cache()
		inputRDD.persist()
		inputRDD.persist(StorageLevel.MEMORY_AND_DISK)

		inputRDD.unpersist()

		inputRDD.count()


		// 为了查看监控，将线程休眠
		Thread.sleep(1000000)

		// TODO: 5、应用结束，关闭资源
		if(!sc.isStopped) sc.stop()
	}

}
