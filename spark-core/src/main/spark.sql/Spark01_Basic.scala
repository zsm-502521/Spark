import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author 赵世敏
 * @date 2022/9/15
 *       1208676641@qq.com
 * @description
 */
object Spark01_Basic {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // TODO: dataFrame
    val df: DataFrame = sparkSession.read.json("datas/user.json")
    df.show()
    // TODO: sql
    //创建临时视图
    df.createTempView("user")
    sparkSession.sql("select * from user").show()
    sparkSession.sql("select avg(age)from user").show()

    println("***************")
    // TODO: dsl
    //使用 dataframe 时 如果涉及到转换操作 需要引入转换规则
    import sparkSession.implicits._
    df.select("age", "username").show()
    df.select($"age" + 1).show()
    println("*********")
    df.select('username).show()
    println("------------")

    // TODO: dataSet
    // df  其实是特定泛型的ds
    val seq = List(("a", 1), ("b", 1))
    val ds: Dataset[(String, Int)] = seq.toDS()
    ds.show()

    // TODO: rdd <=> df
    val rdd: RDD[(Int, String, Int)] = sparkSession.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df1: DataFrame = rdd.toDF("id", "name", "age")
    val rowRDD: RDD[Row] = df1.rdd

    // TODO: df <=> ds
    val ds1: Dataset[User] = df1.as[User]
    ds1.show()
    val df2: DataFrame = ds1.toDF()
    df2.show()
    println("//////////////")

    // TODO: rdd <=> ds
    val ds2: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val rdd1: RDD[User] = ds2.rdd
    println("*****************ds2")
    ds2.show()
    rdd1.collect().foreach(println)

    sparkSession.close()
  }

  case class User(id: Int, name: String, age: Int)
}
