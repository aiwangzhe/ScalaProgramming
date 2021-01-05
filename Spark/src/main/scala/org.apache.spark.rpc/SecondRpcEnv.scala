package org.apache.spark.rpc

import org.apache.spark.SparkConf

import scala.reflect.ClassTag

object SecondRpcEnv {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val securityManager = new org.apache.spark.SecurityManager(sparkConf)
    val rpcEnv = RpcEnv.create("myEnv", "localhost", 0, sparkConf, securityManager, true)
    val pointref = rpcEnv.setupEndpointRef(new RpcAddress("localhost", 8888), "endpoint")
    //implicit val helloTag = ClassTag(classOf[HelloMessage])
    val message = pointref.askSync(new HelloMessage("hahaha"))
    println("message: " + message)

  }

}
