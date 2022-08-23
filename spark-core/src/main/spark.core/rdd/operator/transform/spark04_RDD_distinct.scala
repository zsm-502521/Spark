package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 去重 distinct
 */
object spark04_RDD_distinct {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 distinct
    /*  distinct 源码解读
     map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
                          ||
                          \/
     (1,null),(2,null),(3,null),(4,null),(1,null),(2,null),(3,null),(4,null)
                          ||  k 聚合
                          \/                        map 只要k值
     (1,null)(1,null)... ==> (null,null,...)  ==> null  ==> 1
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))
    val value: RDD[Int] = rdd.distinct()
    value.collect().foreach(println)
    List(1, 2, 1).distinct
    // TODO: 关闭连接
    sc.stop()
  }
}
