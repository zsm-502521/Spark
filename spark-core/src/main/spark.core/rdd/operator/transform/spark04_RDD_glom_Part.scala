package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description glom 分区不变
 */
object spark04_RDD_glom_Part {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 glom
    // 分区计算结果分区不变
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    rdd.saveAsTextFile("output")
    val value: RDD[Int] = rdd.map(_ * 2)
    value.saveAsTextFile("output1")
    value.collect().foreach(println)

    // TODO: 关闭连接
    sc.stop()
  }
}
