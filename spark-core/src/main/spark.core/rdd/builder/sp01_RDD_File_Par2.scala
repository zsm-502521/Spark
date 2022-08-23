package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/13
 *       1208676641@qq.com
 * @description 创建rdd 从文件中  分区
 */
object sp01_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    // TODO: 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO: 创建RDD
    // TODO: 数据分区的分配
    //1 数据以行为单位进行读取
    //   spark读取文件 采用的hadoop的方式读取  一行一行读取 和字节数没有关系
    //2 数据读取时以偏移量为单位
    // TODO: 偏移量不会重复读取
    // TODO: 如果数据源为多个文件 计算分区以文件为单位进行分区

    // 14B /2 =7B   14B /7B =2P
    //偏移量
    /*
                   偏移量
    1234567@@  ==>012345678
    89@@       ==>9 10 11 12
    0          ==>13
                   file
    【0- 7】    ==> 1234567
    【7-14】    ==> 890

     */
    val rdd: RDD[String] = sc.textFile("datas/word.txt", 2)
    rdd.saveAsTextFile("output5")

    // TODO: 关闭环境
    sc.stop()
  }
}
