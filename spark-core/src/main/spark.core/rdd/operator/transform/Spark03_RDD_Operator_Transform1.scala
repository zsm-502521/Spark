package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 分区数据
 */
object Spark03_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO: 算子 mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val value: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(
          num => (index, num)
        )

      }
    )
    value.collect().foreach(println)
    sc.stop()
  }
}
