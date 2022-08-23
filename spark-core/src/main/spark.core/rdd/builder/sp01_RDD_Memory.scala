package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/13
 *       1208676641@qq.com
 * @description 创建rdd 从内存中
 */
object sp01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO: 创建RDD
    //内存中创建  将内存中集合的数据作为处理的数据源
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)

    //parallelize：并行
//    val rdd: RDD[Int] = sc.parallelize(seq)

    //makeRDD 方法实现就是 对象调用的parallelize方法
    val rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(println)

    // TODO: 关闭环境
    sc.stop()
  }
}
