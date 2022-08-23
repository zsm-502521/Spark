package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description  mapPartitions
 */
object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO: 算子 mapPartitions
    /*
      可以以分区为单位进行数据的转换
      但是会将整个分区数据加载到内存进行引用
      数据处理完不会被释放掉 存在对象的引用
      在内存较小 数据量较大的场合 容易存在内存溢出
     */
     /*
     map处理数据不会加载到内存中
      */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val value: RDD[Int] = rdd.mapPartitions(
      i => {
        println("////////////")
        i.map(_ * 2)
      })
    value.collect().foreach(println)
    // TODO: 结束连接
    sc.stop()
  }
}
