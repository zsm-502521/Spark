package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/18
 *       1208676641@qq.com
 * @description Key value值的算子
 */
object Spark05_RDD_KeyValue {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子  k-v类型 partitionBy
    /*
    如果重分区的分区器和当前的分区器一样    会进行分区器校验 不会在执行一次
    分区器 有三种
    可以自己写分区器
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1)) //partitionBy处理的数据是k-v类型
    //隐式转换(二次编译)
    // RDD==> PairRDDFunctions
    // partitionBy  是根据指定的分区规则对数据进行重新分区


    mapRDD.partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output")

    //    rdd.collect().foreach(println)

    // TODO: 关闭连接
    sc.stop()
  }
}
