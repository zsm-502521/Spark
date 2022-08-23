package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 分区修改  coalesce
 */
object spark04_RDD_coalesce {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 coalesce
    /*  coalesce 默认不会将分区数据打乱  可能会导致数据不均衡 （数据倾斜）
       解决方法 : 打乱数据  （shuffle）

     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6),3)
//    val value: RDD[Int] = rdd.coalesce(2,true)

    // TODO: 增加分区
    /*
      coalesce 参数二为false 不会对原数据分区修改
      shuffle 需要为true 如果是false则没有效果

      增加分区  另一个函数
        repartition  底层就是coalesce  默认shuffle 为true

     */

    val value: RDD[Int] = rdd.coalesce(4, true)
    val value1: RDD[Int] = rdd.repartition(4)

    value.saveAsTextFile("output1")

    // TODO: 关闭连接
    sc.stop()
  }
}
