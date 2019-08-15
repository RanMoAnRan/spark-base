import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从HBase表中读取数据，表的名称：ns1:sale_orders
  */
object SparkHBaseRead {

  def main(args: Array[String]): Unit = {

    // a. 构建SparkContext实例对象
    val sparkConf = new SparkConf()
      .setAppName("SparkHBaseRead")
      .setMaster("local[4]")
      // 设置使用Kryo序列
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册哪些类型使用Kryo序列化, 最好注册RDD中类型
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))

    val sc: SparkContext = new SparkContext(sparkConf)


    // b. 读取数据
    /**
      * def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      * conf: Configuration = hadoopConfiguration,
      * fClass: Class[F],
      * kClass: Class[K],
      * vClass: Class[V]
      * ): RDD[(K, V)]
      */
    val conf: Configuration = HBaseConfiguration.create()
    // 设置读HBase表的名称
    conf.set(TableInputFormat.INPUT_TABLE, "ns1:sale_orders")

    // 调用底层API，读取HBase表的数据
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    println(s"Count = ${hbaseRDD.count()}")

    hbaseRDD.take(5).foreach { case (rowKey, result) =>
      println(s"RowKey = ${Bytes.toString(rowKey.get())}")
      for (cell <- result.rawCells()) {
        // 列簇CF
        val cf = Bytes.toString(CellUtil.cloneFamily(cell))
        // 列名称
        val columne = Bytes.toString(CellUtil.cloneQualifier(cell))
        // 列的值
        val value = Bytes.toString(CellUtil.cloneValue(cell))

        println(s"\t ${cf}:${columne} = ${value}, version -> ${cell.getTimestamp}")
      }
    }


    // 应用结束，关闭资源
    sc.stop()
  }

}
