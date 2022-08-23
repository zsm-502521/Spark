package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/18
 *       1208676641@qq.com
 * @description 双value值的不同分区zip
 */
object Spark05_RDD_TwoValue1 {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

    // TODO: 拉链 zip
    //  分区数量要保持一致
    //  分区数量不一样   Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    //  分区中数据数量要保持一致
    //  Can only zip RDDs with same number of elements in each partition

    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)

    println(rdd6.collect().mkString(","))

    //    rdd.collect().foreach(println)

    // TODO: 关闭连接
    sc.stop()
  }
}
