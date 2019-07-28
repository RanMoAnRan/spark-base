import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 单词统计
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //设置spark的配置文件信息
    val sparkConf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    //构建sparkcontext上下文对象
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    //读取文件
    val file: RDD[String] = sparkContext.textFile("spark-01/data/wordcount.data")

    val value: RDD[Int] = sparkContext.makeRDD(1 to 100)
    val value1: RDD[Int] = sparkContext.parallelize(1 to 100)

    sparkContext.setLogLevel("WARN")

    //对文件中每一行单词进行压平切分
    val wc: RDD[(String, Int)] = file.flatMap(x => x.split("\\s+"))
      .map(word => (word, 1))
      //聚合
      .reduceByKey(_ + _)

    wc.foreach(tuple=>println(tuple))

    wc.saveAsTextFile("spark-01/data/output/wc-"+System.currentTimeMillis())

    //关闭资源
    if(!sparkContext.isStopped) sparkContext.stop()

  }

}
