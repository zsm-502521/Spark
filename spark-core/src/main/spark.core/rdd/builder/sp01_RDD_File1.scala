package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/13
 *       1208676641@qq.com
 * @description 创建rdd 从文件中
 */
object sp01_RDD_File1 {
  def main(args: Array[String]): Unit = {
    // TODO: 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO: 创建RDD
    //TextFile 默认以行为单位读取数据
    //wholeTextFile 默认以文件为单位读取数据  读取结果为tuple k= file.path v= file
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas/1.txt")


    rdd.collect().foreach(println)
    // TODO: 关闭环境
    sc.stop()
  }
}
