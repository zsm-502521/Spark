package persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/31
 *       1208676641@qq.com
 * @description  rdd 持久化
 */
object Spark01_RDD_SImple {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(sparkConf)

    // TODO: cache()  将数据临时存储在内存中进行数据重用
    //   persist（）  将数据临时存储在磁盘中  涉及到io  性能低 数据安全
    //                如果job完成 临时数据文件就会删除
    //   checkpoint()   将数据长久的保存在磁盘中 io 性能低 安全
    //                 会独立执行一遍 job


    val list = List("hello scala", "hello Spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    mapRDD.cache()
    mapRDD.checkpoint()
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    println("***************")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
