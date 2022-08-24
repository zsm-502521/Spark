package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/18
 *       1208676641@qq.com
 * @description Key value值的算子  分区聚合  相同规则聚合
 */
object Spark06_RDD_aggregateByKey2 {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 6),
      ("b", 4), ("b", 5), ("b", 3)
    ), 2)
    // TODO: 算子  k-v类型 foldByKey
    /*  存在函数柯里化
     分区内外聚合规则一样
     */
    // 返回值数据类型和初始值一样
    rdd.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)
    // TODO: 分区聚合规则一样 和 reduceByKey 一样
    rdd.reduceByKey(_ + _).collect().foreach(println)
    rdd.foldByKey(0)(_ + _).collect().foreach(println)

    // 获取数据的avg  (a,3)  (b,4)
    val aggRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (k, v) => {
        (k._1 + v, k._2 + 1) //
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    aggRDD.collect().foreach(println)

    val value: RDD[(String, Int)] = aggRDD.mapValues {
      case (n, m) => {
        n / m
      }
    }
    value.collect().foreach(println)
    // TODO: 关闭连接
    sc.stop()

  }
}
