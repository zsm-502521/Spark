package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/13
 *       1208676641@qq.com
 * @description rdd的并行度
 */
object sp01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO: 创建RDD
    //rdd 的并行度 & 分区
    //{1,2}  {3,4}
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2) //{1,2}  {3,4}
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3) //{1} {2,3}  {4,5}

    //将处理后的数据保存成分区文件
    rdd1.saveAsTextFile("output4")

    //    rdd.collect().foreach(println)
    // TODO: 关闭环境
    sc.stop()
  }
}
