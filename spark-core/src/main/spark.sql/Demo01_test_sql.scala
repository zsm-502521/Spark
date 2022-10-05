import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Demo01_test_sql {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","zsm")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use atguigu")

    spark.sql(
      """
        |SELECT t.*
        |         FROM atguigu.city_info t
        |         ORDER BY area DESC
        |""".stripMargin).show(10)
    spark.close()
  }
}
