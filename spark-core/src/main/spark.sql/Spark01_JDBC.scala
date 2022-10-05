import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
 * @author 赵世敏
 * @date 2022/9/15
 *       1208676641@qq.com
 * @description jdbc连接处理数据
 */
object Spark01_JDBC {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    //读取mysql数据
    val df = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/spark_sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load()
    df.show()
    //保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/spark_sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save()

    sparkSession.close()
  }
}
