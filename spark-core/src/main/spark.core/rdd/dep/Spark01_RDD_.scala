package rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author 赵世敏
 * @date 2022/8/30
 *       1208676641@qq.com
 * @description
 */
object Spark01_RDD_ {
  def main(args: Array[String]): Unit = {
    // TODO: 建立与spark框架的连接
    val wordCount: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(wordCount)
    // TODO: 执行业务操作
    //1 读取文件  获取每一行的数据
    val value: RDD[String] = sc.textFile("datas/1.txt")
    println(value.dependencies)
    println(value)
    //2 数据进行拆分    以空格截取数据
    val value1: RDD[String] = value.flatMap(_.split(" "))
    println(value1.toDebugString)
    println(value1)
    //赋值 1
    val wordToOne: RDD[(String, Int)] = value1.map((_, 1))
    //3 数据分组   根据单词分组
    val value2: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(s => s._1)
    println(value2)
    //4 数据转换   计数
    val value3: RDD[(String, Int)] = value2.map {
      case (k, v) => {
        v.reduce(
          (t1, t2) => (t1._1, t2._2 + t2._2)
        )
      }
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
