package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 转换算子
 */
object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO: 算子 map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4)) //==> List(2,4,6,8)

    //转换函数
    def mapFunction(num:Int): Int = {
      num * 2
    }
    val mapRDD1: RDD[Int] = rdd.map(mapFunction)
    val mapRDD2: RDD[Int] = rdd.map((num:Int)=>num*2)
    val mapRDD: RDD[Int] = rdd.map(_*2)
    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
