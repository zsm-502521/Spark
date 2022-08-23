package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/13
 *       1208676641@qq.com
 * @description 创建rdd 从文件中  分区
 */
object sp01_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    // TODO: 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO: 创建RDD
    //textFile 参数解读
    //    minPartitions 最小分区数量  ==> math.min(defaultParallelism, 2)  核数大于2时 默认为 2
    //spark文件分区计算方式底层调用的是hadoop的文件读取方式
    //    totalSize= 7
    //    goalSize = 7 / 2 =3 (byte)
    //    7 / 2 = 2...1 (1.1  (1 > 3(byte)10%) ) +1 = 3 (partition)
    val rdd: RDD[String] = sc.textFile("datas/2.txt", 2)
    rdd.saveAsTextFile("output5")

    // TODO: 关闭环境
    sc.stop()
  }
}
