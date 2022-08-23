package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description glom
 */
object spark04_RDD_glom {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val value: RDD[Array[Int]] = rdd.glom()
   value.collect().foreach(data=> println(data.mkString(",")))
    // TODO: 关闭连接
    sc.stop()
  }
}
