package wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/7/31
 *       1208676641@qq.com
 * @description wc
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // TODO: 建立与spark框架的连接
    val wordCount: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(wordCount)
    // TODO: 执行业务操作
    //1 读取文件  获取每一行的数据
    val value: RDD[String] = sc.textFile("datas/1.txt")
    println(value)
    //2 数据进行拆分    以空格截取数据
    val value1: RDD[String] = value.flatMap(_.split(" "))
    println(value1)
    //3 数据分组   根据单词分组
    val value2: RDD[(String, Iterable[String])] = value1.groupBy(word => word)
    println(value2)
    //4 数据转换   计数
    val value3: RDD[(String, Int)] = value2.map {
      case (k, v) => (k, v.size)
    }
    println(value3)
    //5 数据结果打印
    val tuples: Array[(String, Int)] = value3.collect()
    println(tuples)
    tuples.foreach(println)
    // TODO: 关闭连接
    sc.stop()

  }
}
