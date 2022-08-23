package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description groupBy 首字母排序
 */
object spark04_RDD_groupBy_Test {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 groupBy
    /*
      groupBy 会将数据源的每一个数据进行处理 根据返回的分组key 进行分组
      相同的key的数据会放到同一个组中
     */
     // groupBy会将数据打散 这个操作就是shuffle
    val rdd: RDD[String] = sc.makeRDD(List("hello ","hadoop","scala","spark"),2)
    def groupByFun(str:String): String ={
      str.charAt(0).toString
    }

    val value: RDD[(String, Iterable[String])] = rdd.groupBy(groupByFun)
//    val value: RDD[(String, Iterable[String])] = rdd.groupBy(_.charAt(0))
    value.collect().foreach(println)
    // TODO: 关闭连接
    sc.stop()
  }
}
