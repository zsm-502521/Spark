package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/18
 *       1208676641@qq.com
 * @description 连接  按照相同的key 连接
 */
object Spark07_RDD_join {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    // TODO: 算子
    /*
      按照相同的key 连接
      zip  是按照顺序连接
     */
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    joinRDD.collect().foreach(println)

    // TODO: 关闭连接
    sc.stop()
  }
}
