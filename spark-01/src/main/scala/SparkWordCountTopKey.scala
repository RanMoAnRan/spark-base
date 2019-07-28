import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountTopKey {
  def main(args: Array[String]): Unit = {
    // 构建Spark应用程序入口SparkContext上下文实例对象
    // 对Spark Application进行配置设置，比如设置应用的名称，设置应用运行的地方（--master）
    val sparkConf = new SparkConf()
      .setAppName("SparkWordCount")
      .setMaster("local[2]") // 以本地模式运行
    // 传递SparkConf对象，创建实例
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // 设置日志级别 Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sparkContext.setLogLevel("WARN")

    //读取文件
    val inputRDD: RDD[String] = sparkContext.textFile("spark-01/data/wordcount.data")

    val wordcountsRDD: RDD[(String, Int)] = inputRDD.flatMap(line => line.trim.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)

    wordcountsRDD.foreach(tuple => println(tuple))

    println("===============================================")

    //按照词频count降序排序，获取次数最高的前3个单词

    //方式一 sortbykey
    wordcountsRDD
      // 将二元组中key和value互换
      .map(tuple => tuple.swap)
      .sortByKey(ascending = false)
      .take(3).foreach(text => println(text))

    println("===============================================")

    // 方式二：使用sortBy，指定排序字段和降序排序
    wordcountsRDD.sortBy(tuple => tuple._2, ascending = false)
      .take(3).foreach(tuple => println(tuple))

    println("===============================================")

    // 方式三：直接使用top函数，可以指定排序规则（隐式参数），RDD的数据集少的时候使用，否则OOM
    wordcountsRDD.top(3)(Ordering.by(tuple => tuple._2))
      .foreach(tuple => println(tuple))

    // 应用结束，关闭资源
    if(!sparkContext.isStopped) sparkContext.stop()

  }

}
