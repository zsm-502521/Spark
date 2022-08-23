package wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/7/31
 *       1208676641@qq.com
 * @description wc reduceByKey 聚合
 */
object Spark03_WordCount {
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
    //赋值 1
    val wordToOne: RDD[(String, Int)] = value1.map((_, 1))
    //spark框架提供的一个新功能  分组与聚合使用同一个method
    //reduceByKey : 对相同的 key 的数据  可以对 value 进行reduce 聚合
    //    wordToOne.reduceByKey((x,y)=>x+y)
    val value2: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    //5 数据结果打印
    val tuples: Array[(String, Int)] = value2.collect()
    println(tuples)
    tuples.foreach(println)
    // TODO: 关闭连接
    sc.stop()

  }
}
