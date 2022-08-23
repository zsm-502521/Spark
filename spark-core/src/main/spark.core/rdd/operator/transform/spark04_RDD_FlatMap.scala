package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 扁平化映射
 */
object spark04_RDD_FlatMap {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO: 算子 flatMap
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
    val value: RDD[Int] = rdd.flatMap(
      list => {
        list
      }
    )
    value.collect().foreach(println)
    // TODO: 关闭连接
    sc.stop()
  }
}
