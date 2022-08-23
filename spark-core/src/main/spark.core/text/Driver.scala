package text

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @author 赵世敏
 * @date 2022/8/7
 *       1208676641@qq.com
 * @description 客户端
 */
object Driver {
  def main(args: Array[String]): Unit = {
    //连接服务器
    val client = new Socket("localhost", 9999)
    //输出流
    val out: OutputStream = client.getOutputStream()
    val stream = new ObjectOutputStream(out)

    val task = new Task()
    stream.writeObject(task)
    stream.flush()
    out.close()
    stream.close()

    client.close()
  }
}
