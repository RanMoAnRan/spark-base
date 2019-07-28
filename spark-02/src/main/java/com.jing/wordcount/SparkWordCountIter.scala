package com.jing.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于Spark 会计法编程实现词频统计WordCount程序,修改增加分区
  */
object SparkWordCountIter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getSimpleName)
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val inputRDD: RDD[String] = sc.textFile("G:\\idea-workspace\\spark-base\\spark-01\\data\\wordcount.data", minPartitions = 2)

    sc.setLogLevel("WARN")


    inputRDD.flatMap(line => line.trim.split("\\s+"))
      .mapPartitions {
        iter =>
          // iter 表示RDD中每个分区中的数据，存储在迭代器中，相当于列表List
          iter.map(word => (word, 1))
      }
      .reduceByKey((a, b) => a + b)
      .foreachPartition {
        datas =>
          datas.foreach {
            case (word, count) => println(s"word=${word}, count=${count}")
          }
      }

    if (!sc.isStopped) sc.stop()
  }
}
