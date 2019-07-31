package spark.udf

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUDAF {
  def main(args: Array[String]): Unit = {
    //获取sparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("sparkUDAF").master("local[2]").getOrCreate()
    //通过sparkSession读取json文件得到DataFrame
    val employeeDF: DataFrame = sparkSession.read.json("datas/udaf.txt")
    //通过DataFrame创建临时表
    employeeDF.createOrReplaceTempView("employee_table")
    //注册我们的自定义UDAF函数
    sparkSession.udf.register("avgSal", AvgSalUDAF)
    //调用我们的自定义UDAF函数
    sparkSession.sql("select avgSal(salary) from employee_table").show()

    sparkSession.close()

  }
}
