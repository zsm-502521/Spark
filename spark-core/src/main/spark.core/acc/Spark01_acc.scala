package acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/9/5
 *       1208676641@qq.com
 * @description  累加器
 */
object Spark01_acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // TODO: 累加器
    /*
        获取系统累加器
        spark 提供了 简单数据聚合的累加器
     */
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    val s: Unit = rdd.foreach(
      num => {
        sumAcc.add(num)
      }
    )
    println(s)
    println(sumAcc.value)

    sc.stop()
  }
}
