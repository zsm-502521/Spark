package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/18
 *       1208676641@qq.com
 * @description Key value值的算子
 */
object Spark06_RDD_reduceByKey {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子  k-v类型 reduceByKey
    /*
        对相同的key的数据进行value聚合
        聚合是 两两聚合
        【1，2，3】==>【3，3】 ==>【6】

     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 4), ("a", 2), ("a", 3)))
    //    val value: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
    //      println("x="+x + "  y="+y)
    //      x + y
    //    })
    val value: RDD[(String, Int)] = rdd.reduceByKey(_ + _, 2)

    value.collect().foreach(println)

    // TODO: 关闭连接
    sc.stop()
  }
}
