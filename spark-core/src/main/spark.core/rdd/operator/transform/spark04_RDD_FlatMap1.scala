package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 扁平化映射
 */
object spark04_RDD_FlatMap1 {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO: 算子 flatMap
    val rdd: RDD[String] = sc.makeRDD(List(
      "hello java", "hello spark"
    ))
    val value: RDD[String] = rdd.flatMap(
      s => s.split(" ")
    )
    value.collect().foreach(println)
    // TODO: 关闭连接
    sc.stop()
  }
}
