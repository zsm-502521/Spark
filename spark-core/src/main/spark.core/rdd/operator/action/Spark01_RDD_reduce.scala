package rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 赵世敏
 * @date 2022/8/25
 *       1208676641@qq.com
 * @description 行动算子
 */
object Spark01_RDD_reduce {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top3")
    val sc = new SparkContext(sparkConf)
    // TODO: 行动算子  触发作业（job）执行的方法
    /*    调用了环境对象的 runJob 方法

     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //    rdd.saveAsTextFile("output")
    //    rdd.saveAsObjectFile("output1")
    // savaASSer   处理的数据是k-v 类型

    val user = new User
    // TODO: Task not serializable
    // NotSerializableException: rdd.operator.action.Spark01_RDD_reduce$User
    rdd.foreach(
      num =>{
        println("age "+user.age+num)  //未序列化
      }
    )


    sc.stop()
  }

  class User extends Serializable {
    var age: Int = 20
  }
}

