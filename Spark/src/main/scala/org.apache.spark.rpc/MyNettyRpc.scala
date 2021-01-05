package org.apache.spark.rpc

import org.apache.spark.SparkConf


object MyNettyRpc {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val securityManager = new org.apache.spark.SecurityManager(sparkConf)
    val rpcEnv = RpcEnv.create("myEnv", "localhost", 8888, sparkConf, securityManager)
    val endpoint = new DEndPoint(rpcEnv);
    rpcEnv.setupEndpoint("endpoint", endpoint)
    while (true) {

    }
  }
}
