package partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/9/5
 *       1208676641@qq.com
 * @description 自定义分区器
 */
object Spark01_Partitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("a", "123"),
      ("c", "123"),
      ("b", "123"),
      ("a", "123")
    ))
    val parRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitionner)
    parRDD.saveAsTextFile("output")
    sc.stop()
  }

}
/*
    自定义分区步骤
      1继承Partitioner
      2重写

 */
class MyPartitionner extends Partitioner{
  //分区数量
  override def numPartitions: Int = 3
  //放回数据的分区索引
  override def getPartition(key: Any): Int = {
    if(key =="a"){
      0
    }else if (key== "b"){
      1
    }else{
      2
    }

  }
}
