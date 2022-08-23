package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 抽取 sample
 */
object spark04_RDD_sample {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 sample
    /*  sample参数解读
      第一个参数  抽取完数据是否将数据放回 true（放回）/ false
      第二个参数  数据源中被抽取的概率
                  第一个参数为 false 时 基准值的概念
                             true 时 每条数据被抽取的可能次数
      第三个参数  抽取数据时随机数的种子
                不传递 默认为 当前系统时间
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    val value: RDD[Int] = rdd.sample(
      false,
      0.1
    )
    val value1: RDD[Int] = rdd.sample(
      true,
      2
    )
    value.collect().foreach(print)
    println()
    value1.collect().foreach(print)
    // TODO: 关闭连接
    sc.stop()
  }
}
