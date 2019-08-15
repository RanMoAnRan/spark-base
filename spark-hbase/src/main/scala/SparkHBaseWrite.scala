import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 向HBase表中写入数据
  */
object SparkHBaseWrite {

  def main(args: Array[String]): Unit = {


    // a. 构建SparkContext实例对象
    val sparkConf = new SparkConf()
      .setAppName("SparkHBaseWrite")
      .setMaster("local[4]")
      // 设置使用Kryo序列
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册哪些类型使用Kryo序列化, 最好注册RDD中类型
      .registerKryoClasses(Array(classOf[Put]))

    val sc: SparkContext = new SparkContext(sparkConf)

    // b. 模拟数据集
    val inputRDD: RDD[(String, Int)] = sc.parallelize(
      List(("spark", 233), ("hadoop", 123), "flink" -> 300), numSlices = 2
    )


    // c. HBase 表的设计： htb_wordcount  info  Rowkey=word     info:count
    // TODO: 将存储数据到HBase表中的RDD进行类型转换，RDD[(ImmutableBytesWritable, Put)]
    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = inputRDD.map { case (word, count) =>
      // 构建HBase表的RowKey
      val rowKey = new ImmutableBytesWritable(Bytes.toBytes(word + ""))

      // 构建Put对象, 传递rowkey，表示向HBase表的哪行插入数据
      val put = new Put(rowKey.get())
      // 设置列的值
      put.addColumn(
        Bytes.toBytes("info"),
        Bytes.toBytes("count"),
        Bytes.toBytes(count + "")
      )

      // 返回二元组
      (rowKey, put)
    }


    /*
     def saveAsNewAPIHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration = self.context.hadoopConfiguration
     ): Unit
     */
    val conf: Configuration = HBaseConfiguration.create()
    // 设置数据输出表的名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_wordcount")

    putsRDD.saveAsNewAPIHadoopFile(
      "/spark/hbase/write/xxx" + System.currentTimeMillis(),
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )

    // 应用结束，关闭资源
    sc.stop()
  }

}
