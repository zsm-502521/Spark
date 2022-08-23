package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/18
 *       1208676641@qq.com
 * @description Key value值的算子  分区聚合  可以采用不同的规则
 */
object Spark06_RDD_aggregateByKey {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ), 2)
    // TODO: 算子  k-v类型 aggregateByKey
    /*  存在函数柯里化
    第一个参数 初始值
        用于和第一个数据key 时 和value 分区内计算
    第二个参数 （两个参数） 计算规则
      第一个参数  分区内的计算规则
      第二个参数  分区间的计算规则
     */
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y

    )
      .collect().foreach(println)

    // TODO: 关闭连接
    sc.stop()
  }
}
