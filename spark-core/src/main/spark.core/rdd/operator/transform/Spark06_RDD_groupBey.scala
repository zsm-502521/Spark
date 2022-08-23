package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/18
 *       1208676641@qq.com
 * @description Key value值的算子
 */
object Spark06_RDD_groupBey {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子  k-v类型 groupByKey
    /*
    只对kay 分组
    相同的key的数据 分在一个组中  形成 一个对偶元组 ==（key,相同key的value的集合）
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 4), ("a", 2), ("a", 4)))
    val value: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    value.collect().foreach(println)
    // 可以对任意值 分组
    val value1: RDD[(Int, Iterable[(String, Int)])] = rdd.groupBy(_._2)
    value1.collect().foreach(println)

    // TODO: 关闭连接
    sc.stop()
  }
}
