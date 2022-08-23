package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 从服务器日志apache.log 获取用户请求的 url 资源路径
 */
object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO: 算子 map
    val value: RDD[String] = sc.textFile("datas/apache.log")
    //长的字符串转换为短字符
    val mapRDD: RDD[String] = value.map(
      line => {
        val strings: Array[String] = line.split(" ")
        strings(6)
      }
    )
    mapRDD.collect().foreach(println)
    // TODO: 结束连接
    sc.stop()
  }
}
