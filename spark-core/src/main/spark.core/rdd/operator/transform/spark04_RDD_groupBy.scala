package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description groupBy
 */
object spark04_RDD_groupBy {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 groupBy
    /*
      groupBy 会将数据源的每一个数据进行处理 根据返回的分组key 进行分组
      相同的key的数据会放到同一个组中
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    def groupByFun(num:Int):String ={
      if (num % 2 == 0) {"偶数"
      }else {
        "奇数"
      }
    }

    val value: RDD[(String, Iterable[Int])] = rdd.groupBy(groupByFun)
    value.collect().foreach(println)
    // TODO: 关闭连接
    sc.stop()
  }
}
