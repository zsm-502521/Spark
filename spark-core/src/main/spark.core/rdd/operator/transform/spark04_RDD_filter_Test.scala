package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 获取某个时间段的数据
 */
object spark04_RDD_filter_Test {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 filter
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    val value: RDD[String] = rdd.filter(
      line => {
        val datas: Array[String] = line.split(" ")
        val times: String = datas(3)
        times.startsWith("17/05/2015") // times 是以这个开头的话就保留
      }
    )

//    val value1: RDD[String] = value.filter(
//      line1 => {
//        val datas1: Array[String] = line1.split(" ")
//        val str: String = datas1(3)
//        str.startsWith(" ")
//
//      }
//    )
    value.collect().foreach(println)

    // TODO: 关闭连接
    sc.stop()
  }
}
