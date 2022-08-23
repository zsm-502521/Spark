package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/18
 *       1208676641@qq.com
 * @description   双value值的算子  交集 并集 差集
 */
object Spark05_RDD_TwoValue {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
    val rdd: RDD[String] = sc.makeRDD(List("j", "a", "v", "a"))
    // TODO: 交集  intersection [3,4]
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
//    val rdd33: RDD[Int] = rdd1.intersection(rdd)  error  数据类型不一样
    println(rdd3.collect().mkString(","))
    // TODO: 并集  union [1,2,3,4,5,6]
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))
    // TODO: 差集  subtract [1,2]
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)  // 站在rdd1的角度上 除去一样的
    println(rdd5.collect().mkString(","))
    // TODO: 拉链 zip  类型可以不一样
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    val value: RDD[(Int, String)] = rdd1.zip(rdd)
    println(rdd6.collect().mkString(","))
    println(value.collect().mkString(","))
//    rdd.collect().foreach(println)

    // TODO: 关闭连接
    sc.stop()
  }
}
