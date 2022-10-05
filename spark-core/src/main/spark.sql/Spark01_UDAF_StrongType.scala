import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/**
 * @author 赵世敏
 * @date 2022/9/15
 *       1208676641@qq.com
 * @description 自定义UDAF函数  强类型的
 */
object Spark01_UDAF_StrongType {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // TODO: dataFrame
    val df: DataFrame = sparkSession.read.json("datas/user.json")
    // TODO: sql
    //创建临时视图
    df.createTempView("user")
    sparkSession.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))
    sparkSession.sql("select ageAvg(age) from user").show()
    sparkSession.close()
  }

  //自定义聚合函数类 计算avg（age）
  /*
    1 继承 org.apache.spark.sql.expressions.Aggregator[]
      IN  输入的数据类型 Long
      BUF 缓冲区的数据类型
      OUT 输出的数据类型 Long

    2 重写方法(6)
   */
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    //z & zero 初始值或者0 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //根据输入的数据来更新缓冲区的数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    //合并分区间数据
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      //buff1 第一个分区的数据
      buff1.total = buff1.total + buff2.total
      //buff2 其他分区的数据
      buff1.count = buff1.count + buff2.count
      buff1
    }

    //计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    //缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
