import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Demo01_2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "zsm")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use atguigu")
    // TODO: 执行操作
    spark.sql(
      """
        |select *
        |from (select *,
        |             rank() over (partition by area order by clickCnt desc ) as rank
        |      from (
        |               select area,
        |                      product_name,
        |                      count(*) as clickCnt
        |               from (
        |                        select u.*,
        |                               p.product_name,
        |                               c.city_name,
        |                               c.area
        |                        from user_visit_action u
        |                                 join product_info p on p.product_id = u.click_product_id
        |                                 join city_info c on u.city_id = c.city_id
        |                        where u.click_product_id > -1
        |                    ) t1
        |               group by area, product_name
        |           ) t2
        |     ) t3
        |where rank <= 3;
        |""".stripMargin).show()
    spark.close()
  }
}
