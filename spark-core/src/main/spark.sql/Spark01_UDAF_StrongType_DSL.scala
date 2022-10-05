import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
 * @author 赵世敏
 * @date 2022/9/15
 *       1208676641@qq.com
 * @description 自定义UDAF函数  强类型的 早期强类型使用 Aggregate
 */
object Spark01_UDAF_StrongType_DSL {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    // TODO: dataFrame
    val df: DataFrame = sparkSession.read.json("datas/user.json")

    /*
    早期的spark版本中不能在sql中使用 强类型的 UDAF 操作
     所以UDAF强类型聚合函数 使用DSL 语法操作
     */
    // TODO: dsl
    val ds: Dataset[User] = df.as[User]
    //将udaf函数转换为查询的列对象
    val column: TypedColumn[User, Long] = new MyAvgUDAF().toColumn
    ds.select(column).show()

//    // TODO: sql
//    //创建临时视图
//    df.createTempView("user")
//    sparkSession.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))
//    sparkSession.sql("select ageAvg(age) from user").show()

    sparkSession.close()
  }

  //自定义聚合函数类 计算avg（age）
  /*
    1 继承 org.apache.spark.sql.expressions.Aggregator[]
      IN  输入的数据类型 User
      BUF 缓冲区的数据类型
      OUT 输出的数据类型 Long

    2 重写方法(6)
   */
  case class User(username:String,age:Int)
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[User, Buff, Long] {
    //z & zero 初始值或者0 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //根据输入的数据来更新缓冲区的数据
    override def reduce(buff: Buff, in: User): Buff = {
      buff.total = buff.total + in.age
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
