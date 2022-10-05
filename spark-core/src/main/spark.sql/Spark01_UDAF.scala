import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author 赵世敏
 * @date 2022/9/15
 *       1208676641@qq.com
 * @description 自定义UDAF函数  弱类型的 （sql操作）
 */
object Spark01_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    // TODO: dataFrame
    val df: DataFrame = sparkSession.read.json("datas/user.json")
    // TODO: sql
    //创建临时视图
    df.createTempView("user")
    sparkSession.udf.register("avgAge", new MyAvgUDAF())
    sparkSession.sql("select avgAge(age) from user").show()

    sparkSession.close()
  }

  //自定义聚合函数类 计算avg（age）
  /*
    1 继承 UserDefinedAggregateFunction
    2 重写方法
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction {
    //输入数据结构
    override def inputSchema: StructType = {
      StructType(Array(StructField("age", LongType)))
    }

    //缓冲区数据的结构
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        ))
    }

    //输出结果的数据类型
    override def dataType: DataType = LongType

    //函数的稳定性
    override def deterministic: Boolean = true

    //缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      //      buffer(0)= 0L
      //      buffer(1)= 0L
      //两种方法一样
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    //根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    //缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    //计算avg
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

}
