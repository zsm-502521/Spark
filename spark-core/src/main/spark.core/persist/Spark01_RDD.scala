package persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/31
 *       1208676641@qq.com
 * @description
 */
object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(sparkConf)

    val list = List("hello scala", "hello Spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    println("***************")

    val list1 = List("hello scala", "hello Spark")
    val rdd1: RDD[String] = sc.makeRDD(list)
    val flatRDD1: RDD[String] = rdd1.flatMap(_.split(" "))
    val mapRDD1: RDD[(String, Int)] = flatRDD1.map((_, 1))
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD1.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
