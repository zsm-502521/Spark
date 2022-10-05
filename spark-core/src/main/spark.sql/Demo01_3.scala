import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * sql分解与 UDTF 函数编写
 */
object Demo01_3 {
  def main(args: Array[String]): Unit = {
    //设置访问者
    System.setProperty("HADOOP_USER_NAME", "zsm")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use atguigu")
    // TODO: 执行操作

    //查询基本数据
    spark.sql(
      """
        |select
        |     a.click_product_id,
        |     a.city_id,
        |     b.product_name,
        |     c.area,
        |     c.city_name
        |from user_visit_action a
        |     join product_info b on b.product_id = a.click_product_id
        |     join city_info c on a.city_id = c.city_id
        |where a.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")

    //注册UDAF
//    spark.udf.register("cityRemark")

    //根据区域 ，商品进行数据的聚合
    spark.sql(
      """
        |select
        |     area,
        |     product_name,
        |     count(*) as click
        |from t1
        |group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    //分区排序
    spark.sql(
      """
        |select
        |     area,
        |     product_name,
        |     click,
        |     rank() over (partition by area order by click desc ) as rank
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")

    //选前三
    spark.sql(
      """
        |select
        |     area,
        |     product_name,
        |     click,rank
        |from t3
        |where rank <=3
        |""".stripMargin).show(false)
    spark.close()
  }
}
