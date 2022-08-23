package text

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @author 赵世敏
 * @date 2022/8/7
 *       1208676641@qq.com
 * @description 服务器端
 */
object Executor {
  def main(args: Array[String]): Unit = {
    //启动服务器 接收数据
    val server = new ServerSocket(9999)
    println("服务器启动，等待接收数据")
    //等待客户端连接
    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    val stream = new ObjectInputStream(in)
    val task: Task = stream.readObject().asInstanceOf[Task]
    val ints: List[Int] = task.compute()
    println("计算节点结果: " + ints)
    in.close()
    stream.close()
    client.close()
    server.close()

  }
}
