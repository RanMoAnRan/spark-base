package spark.agg

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用Spark实现对数据分组、排序和TopKey操作，分别使用groupByKey和aggregateByKey函数。
  */
object SparkGroupTopKey {

  def main(args: Array[String]): Unit = {

    // 1、创建SparkContext实例对象
    val sc: SparkContext = {
      // 创建SparkConf配置应用的属性
      val sparkConf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("SparkGroupTopKey")
      val context = SparkContext.getOrCreate(sparkConf)
      // 设置日志级别
      context.setLogLevel("WARN")
      // 返回
      context
    }

    // 2、读取数据，从HDFS上读取数据
    val inputRDD: RDD[String] = sc.textFile("datas/group/group.data", minPartitions = 2)


    // 3、将数据进行转换为Key-Value类型的RDD
    val datasRDD: RDD[(String, Int)] = inputRDD
      .filter(line => null != line && line.trim.split("\\s").length >= 2)
      .mapPartitions { iter =>
        iter.map { line =>
          // 按照空格对数据进行分割
          val Array(word, number) = line.trim.split("\\s")
          // 直接以二元组形式返回
          (word, number.toInt)
        }
      }


    // TODO: 4、对数据进行分组、排序和TopKey操作，使用groupByKey完成
    datasRDD
      .groupByKey() // 直接按照Key进行分组  RDD[(String, Iterable[Int])]
      // 对每个分组中的数据进行排序，获取TopKey
      .mapPartitions { iter =>
      //  iter表示的是RDD中各个分区的数据，以迭代器的形式存在，对其中的数据进行处理操作
      // val yy: Iterator[(String, Iterable[Int])] = iter
      iter.map { case (word: String, numbers: Iterable[Int]) =>
        // 对组内的数据进行降序排序
        (word, numbers.toList.sortBy(x => -x).take(3))
      }
    }
      .foreachPartition(iter => iter.foreach(item => println(item)))


    // TODO: 5、对数据进行分组、排序和TopKey操作，使用aggregateByKey完成
    /*
      def aggregateByKey[U: ClassTag](zeroValue: U)
      (
        seqOp: (U, V) => U,
          combOp: (U, U) => U
          ): RDD[(K, U)]
          依据此需求分析可知：
              聚合中间临时变量的类型为可变的列表ListBuffer（依据聚合的结果数据类型倒推）
     */
    datasRDD
      // 按照Key分组，对组内数据进行“聚合”操作，此处获取每个组内最大的三个元素
      .aggregateByKey(new ListBuffer[Int]())(
      // 分区内的聚合 seqOp: (U, V) => U
      (u: ListBuffer[Int], number: Int) => {
        u += number
        u.sortBy(-_).take(3)
      },
      // 分区间的聚合, combOp: (U, U) => U
      (u1: ListBuffer[Int], u2: ListBuffer[Int]) => {
        u1 ++= u2
        u1.sortBy(-_).take(3)
      }
    )
      .foreachPartition(iter => iter.foreach(item => println(item)))


    // 线程休眠
    Thread.sleep(10000000)

    // 当应用结束，关闭资源
    sc.stop()
  }

}
