package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/13
 *       1208676641@qq.com
 * @description 创建rdd 从文件中
 */
object sp01_RDD_File {
  def main(args: Array[String]): Unit = {
    // TODO: 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO: 创建RDD
    //文件中创建  将文件中的数据作为处理的数据源
    //path路径默认以当前pwd 为基准 可以写绝对路径也可以写相对路径
    //读取多个文件可以直接写目录path
//    val rdd: RDD[String] = sc.textFile("datas/1.txt")
//    val rdd: RDD[String] = sc.textFile("datas")
    //可以使用通配符
//    val rdd: RDD[String] = sc.textFile("datas/1*.txt")
    //可以使用hdfs‘s path
    val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:8020/1.txt")

    rdd.collect().foreach(println)
    // TODO: 关闭环境
    sc.stop()
  }
}
