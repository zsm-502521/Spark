import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author 赵世敏
 * @date 2022/9/15
 *       1208676641@qq.com
 * @description   idea连接外置hive
 */
object Spark01_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val sparkSession: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    // TODO: spark连接外置hive
    /*
      1. 拷贝hive.site 文件到classpath下
      2. 启用hive的支持 enableHivesupport
      3. 增加对应的依赖 ：mysql
     */
    sparkSession.sql("show tables").show()

    // TODO: 关闭连接
    sparkSession.close()
  }
}
