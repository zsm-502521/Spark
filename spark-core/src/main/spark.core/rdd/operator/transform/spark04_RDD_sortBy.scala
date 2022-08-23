package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description 排序 sortBy
 */
object spark04_RDD_sortBy {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 sortBy
    /*
    不对改变分区数量 但是会进行shuffle操作
     参数一 指定规则对数据进行排序
     参数二 默认为升序  false 为降序
     */
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    val value: RDD[(String, Int)] = rdd.sortBy(kv => kv._1.toInt,false)


    value.collect().foreach(println)
    // TODO: 关闭连接
    sc.stop()
  }
}
