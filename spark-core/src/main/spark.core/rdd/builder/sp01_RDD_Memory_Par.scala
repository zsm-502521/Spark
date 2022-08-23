package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/13
 *       1208676641@qq.com
 * @description rdd的并行度
 */
object sp01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    // TODO: 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism","5")
    val sc = new SparkContext(sparkConf)
    // TODO: 创建RDD
    //rdd 的并行度 & 分区
    //makeRDD 参数二 numSlices 表示分区数量  默认== defaultParallelism
    //    默认为 scheduler.conf.getInt("spark.default.parallelism",totalCores)
    //    默认从conf中获取spark.default.parallelism   如果获取不到则使用 totalCores 属性
    //    totalCores 值为当前运行环境最大可用核数
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //将处理后的数据保存成分区文件
    rdd1.saveAsTextFile("output2")


//    rdd.collect().foreach(println)
    // TODO: 关闭环境
    sc.stop()
  }
}
