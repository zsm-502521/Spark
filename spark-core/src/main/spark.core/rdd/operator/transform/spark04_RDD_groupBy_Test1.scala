package rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author 赵世敏
 * @date 2022/8/14
 *       1208676641@qq.com
 * @description groupBy  获取每个时间段访问量
 */
object spark04_RDD_groupBy_Test1 {
  def main(args: Array[String]): Unit = {
    // TODO: 创建连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)

    // TODO: 算子 groupBy
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ") // 取每行按照空格 split index == 3 result
        val times: String = datas(3)
        //times.substring(0)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(times)
        val sdf1 = new SimpleDateFormat("HH")
        val hours: String = sdf1.format(date)
        (hours, 1)
      }
    ).groupBy(_._1)
    timeRDD.map{
      case (hour,iter)=>{
        (hour,iter.size)
      }
    }.collect().foreach(println)

    // TODO: 关闭连接
    sc.stop()
  }
}
