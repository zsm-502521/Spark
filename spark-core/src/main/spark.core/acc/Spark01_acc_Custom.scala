//package acc
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.util.AccumulatorV2
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable
//
///**
// * @author 赵世敏
// * @date 2022/9/5
// *       1208676641@qq.com
// * @description 自定义累加器 wc
// */
//object Spark01_acc_Custom {
//  def main(args: Array[String]): Unit = {
//    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
//    val sc = new SparkContext(sparkConf)
//
//    val rdd: RDD[String] = sc.makeRDD(List("hello", "java", "java"))
//    //    rdd.map((_, 1)).reduceByKey(_+_)
//
//    // TODO: 自定义累加器
//    //创建累加器对象
//    val wcAcc = new MyAccumulatot
//    //向spark 进行注册
//    sc.register(wcAcc,"wordCount")
//    //自定义累加器  wordcount  的累加器
//
//    rdd.foreach(
//      word => {
//        //数据的累计`
//        wcAcc.add(word)
//      }
//    )
//    println(wcAcc.value)
//    sc.stop()
//  }
//
//  /*
//    继承 AccumulatorV2
//      IN 累加器输入的内容type
//      out 累加器返回的数据类型   mutable.Map[String,Long]
//   */
//  class MyAccumulatot extends AccumulatorV2 [String,mutable.Map[String,Long]]{
//    private var wcMap = mutable.Map[String,Long]()
//
//    override def isZero: Boolean = ???
//
//    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = ???
//
//    override def reset(): Unit = ???
//
//    //获取累加器需要的值
//    override def add(word: String): Unit ={
//     val newCount =wcMap.getOrElse(word,0L)+ 1
//      wcMap.update(word,newCount)
//
//    }
//
//    //driver 合并多个累加器
//    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
////      val wcMap1=this.wcMap
////      val wcMap2=other.value
////      wcMap2.foreach(
////        case ()
////      )
//    }
//
//    //累加器结果
//    override def value: mutable.Map[String, Long] = {
//      wcMap
//    }
//
//  }
//}
