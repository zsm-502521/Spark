package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 并行计算
 */
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO: 算子 map
    /* 一个分区
      rdd 的计算一个分区内的数据是一个一个的执行逻辑
        只有前面的数据全部执行结束之后  才会执行下一个数据
        分区内数据执行是有序的
     */
    /* 不同分区
      数据计算是无序的
     */

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val value: RDD[Int] = rdd.map(num => {
      println("<<<<<<<<<<<<" + num)
      num
    })
    val value1: RDD[Unit] = value.map(num => {
      println("*************" + num)
      num
    })
    value1.collect()
    // TODO: 结束连接
    sc.stop()
  }
}
