package rdd.Demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/24
 *       1208676641@qq.com
 * @description 统计出每一个省份 每个广告被点击数量排行的 Top3
 */
object Top3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top3")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("datas/agent.log")
    //数据准备
    /*
         时间戳，     【省份】，城市，用户，【广告】
       1516609143871   4      0    32    20
       1516609143871   2      1    28    26
       1516609143871   5      2    68    24
     */

    //  只要省份 和广告  (3,5) (3,26) (5,24)
    val mapRDD: RDD[(String, String)] = rdd.map(
      data => {
        val line: Array[String] = data.split(" ")
        (line(1), line(4))
      }
    )
    val mapRDD1: RDD[((String, String), Int)] = mapRDD.map {
      case (k, v) => {
        ((k, v), 1)
      }
    }
    val reduceRDD: RDD[((String, String), Int)] = mapRDD1.reduceByKey(_ + _)
    /*
    ((1,25),15)
    ((0,7),5)
    ((5,10),15)
     */
    val reduceRDD1: RDD[(String, (String, Int))] = reduceRDD.map {
      case (k, v) =>
        (k._1, (k._2, v))

    }
    /*
    (1,(25,15))
    (0,(7,5))
    (5,(10,15))
     */
    // 分组 而不是 聚合
    val GroupRDD: RDD[(String, Iterable[(String, Int)])] = reduceRDD1.groupByKey()
    /*
    (4,CompactBuffer((12,25), (25,11), (27,13), (13,21), (8,21), (1,20), (6,15), (7,7), (3,15), (22,18), (10,10), (19,10), (9,19), (23,18), (18,17), (11,11), (28,16), (26,10), (2,22), (29,13), (17,14), (16,22), (5,14), (21,17), (0,14), (15,17), (24,18), (4,17), (20,11), (14,18)))
    (8,CompactBuffer((8,18), (13,19), (16,10), (17,17), (23,14), (19,18), (24,17), (2,27), (9,20), (7,19), (11,22), (0,17), (5,19), (21,15), (18,18), (1,13), (4,16), (25,12), (15,17), (29,17), (14,18), (28,15), (3,18), (10,15), (12,18), (26,13), (6,13), (20,23), (22,11), (27,18)))

     */
    // TODO:  scala 中list sortBy 默认是升序
    GroupRDD.mapValues(
      iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )
      /*    iter toList 的result
      (4,List((12,25), (25,11), (27,13), (13,21), (8,21), (1,20), (6,15), (7,7), (3,15), (22,18), (10,10), (19,10), (9,19), (23,18), (18,17), (11,11), (28,16), (26,10), (2,22), (29,13), (17,14), (16,22), (5,14), (21,17), (0,14), (15,17), (24,18), (4,17), (20,11), (14,18)))
      (8,List((8,18), (13,19), (16,10), (17,17), (23,14), (19,18), (24,17), (2,27), (9,20), (7,19), (11,22), (0,17), (5,19), (21,15), (18,18), (1,13), (4,16), (25,12), (15,17), (29,17), (14,18), (28,15), (3,18), (10,15), (12,18), (26,13), (6,13), (20,23), (22,11), (27,18)))

       */
      .collect().foreach(println)

    sc.stop()
  }
}
