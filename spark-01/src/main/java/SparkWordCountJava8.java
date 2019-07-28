import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author RanMoAnRan
 * @ClassName: SparkWordCountJava8
 * @projectName spark-base
 * @description: 使用Java语言（Lambda 表达式），基于Spark框架实现词频统计WordCount
 * @date 2019/7/26 21:11
 */
public class SparkWordCountJava8 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[2]");
        //构建Spark应用程序入口JavaSparkContext上下文实例对象
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        sparkContext.setLogLevel("WARN");

        JavaRDD<String> textFile = sparkContext.textFile("spark-01/data/wordcount.data");

        JavaPairRDD<String, Integer> wordcountsRDD = textFile.flatMap(line -> Arrays.asList(line.trim().split("\\s+")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((v1, v2) -> v1 + v2);


        wordcountsRDD.foreach(tuple -> System.out.println(tuple.toString()));

    }
}
