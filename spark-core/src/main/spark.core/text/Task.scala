package text

/**
 * @author 赵世敏
 * @date 2022/8/7
 *       1208676641@qq.com
 * @description  计算任务
 */
/*
对象在传输过程中需要序列化 混入特质
 */
class Task extends Serializable {
  //数据
  val datas = List(1, 2, 3, 4)
  //function
  val logic = (num: Int) => num * 2

  //计算
  def compute () ={
    datas.map(logic)
  }
}
