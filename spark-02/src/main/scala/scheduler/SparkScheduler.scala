package scheduler

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用Scala语言，基于Spark 会计法编程实现词频统计WordCount程序
  */
object SparkScheduler {

	// TODO: Spark Application应用入门，Driver Program，必须创建SparkContext实例对象
	def main(args: Array[String]): Unit = {

		// TODO: 1、构建Spark应用程序入口SparkContext上下文实例对象
		// 对Spark Application进行配置设置，比如设置应用的名称，设置应用运行的地方（--master）
		val sparkConf = new SparkConf()
	    	.setAppName("SparkScheduler")
	    	.setMaster("local[2]") // 以本地模式运行岑石
		// 传递SparkConf对象，创建实例
		val sc: SparkContext = new SparkContext(sparkConf)

		// TODO: 2、读取要处理的数据，从本地文件系统读取（LocalFS）
		val inputRDD: RDD[String] = sc.textFile("datas/wordcount/input/wordcount.data")

		// TODO: 3、处理数据，调用RDD集合中函数（类比于Scala集合类中列表List）
		val wordcountsRDD: RDD[(String, Int)] = inputRDD
			// 将每行数据按照分隔符进行分割，将数据扁平化
	    	.flatMap(line => line.trim.split("\\s+"))
			// 将每个单词转换为二元组，表示该单词出现一次
	    	.map(word => (word, 1))
			// 按照Key聚合统计, 先按照Key分组，再聚合统计（此函数局部聚合，再进行全局聚合）
	    	.reduceByKey((a, b) => a + b )

		// TODO: 4、输出结果RDD到本地文件系统
		wordcountsRDD.foreachPartition(iter => iter.foreach(println))

		// 为了查看监控，将线程休眠
		Thread.sleep(1000000)

		// TODO: 5、应用结束，关闭资源
		if(!sc.isStopped) sc.stop()
	}

}
