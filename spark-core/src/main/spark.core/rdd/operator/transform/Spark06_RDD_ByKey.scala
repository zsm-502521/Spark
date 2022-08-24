package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/18
 *       1208676641@qq.com
 * @description Key value值的算子
 */
object Spark06_RDD_ByKey {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ), 2)
    // TODO: 算子  k-v类型 ByKey

    rdd.aggregateByKey(0)((x, y) => math.max(x, y), (x, y) => x + y)
      .collect().foreach(println)
    /*
      底层
          combineByKeyWithClassTag[C](
          createCombiner: V => C,         //相同key 的第一条数据进行的处理
          mergeValue: (C, V) => C,        //分区内 数据的处理函数
          mergeCombiners: (C, C) => C,    //分区间 数据的处理函数
       )   //

       combineByKey
       reduceByKey
       aggregateByKey
       foldByKey   分区内间 规则一样
     */
    // wordcount
    rdd.reduceByKey(_+_)
    rdd.aggregateByKey(0)(_+_,_+_)
    rdd.foldByKey(0)(_+_)
    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y)
    // TODO: 关闭连接
    sc.stop()
  }
}
