import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame,SparkSession}

/**
 * @author 赵世敏
 * @date 2022/9/15
 *       1208676641@qq.com
 * @description
 */
object Spark01_UDF {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    // TODO: dataFrame
    val df: DataFrame = sparkSession.read.json("datas/user.json")
    // TODO: sql
    //创建临时视图
    df.createTempView("user")
    sparkSession.udf.register("prefixName",(name:String) => {
      "Name: " +name
    })
    sparkSession.sql("select age,prefixName(username) from user").show()

    sparkSession.close()
  }

}
