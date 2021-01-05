package org.apache.spark.rpc

case class HelloMessage(msg: String)

class DEndPoint(val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case HelloMessage(msg) => {
      println("received msg: " + msg)
      val result = "I am received!"
      context.reply(result)
    }
  }
}
