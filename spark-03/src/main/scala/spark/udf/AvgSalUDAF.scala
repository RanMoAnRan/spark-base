package spark.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
  * 自定义UDAF
  * //用于自定义聚合函数：实现求取平均工资
  */
object AvgSalUDAF extends UserDefinedAggregateFunction {
  //表示此聚合函数输入参数类型
  override def inputSchema: StructType = {
    StructType(StructField("input", LongType) :: Nil)
  }

  //表示聚合时缓冲的（中间临时变量）数据类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("total", LongType) :: Nil)
  }

  //表示此聚合函数输出结果的数据类型
  override def dataType: DataType = DoubleType

  //表示唯一性
  override def deterministic: Boolean = true

  //表示对缓冲数据的初始化（中间临时变量的初始化）
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //局部聚合：对各个分区数据聚合操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //所有的金额相加
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    //一共有多少条数据
    buffer(1) = buffer.getLong(1) + 1
  }

  //全局聚合：对各个分区聚合结果数据聚合操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //表示：聚合函数最终如何通过缓冲数据（聚合中间临时变量）得到最终输出结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
