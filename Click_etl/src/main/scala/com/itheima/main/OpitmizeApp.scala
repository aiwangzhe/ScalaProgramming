package com.itheima.main

import com.itheima.bean.{WebLogBean, WeblogBeanCase}
import com.itheima.service.PageViewService
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object OpitmizeApp {

  def main(args: Array[String]): Unit = {
    //准备sparksession
    val conf = new SparkConf()
    val spark = SparkSession.builder()
      .master("local[*]")
      .config(conf).appName(ETLApp.getClass.getName).getOrCreate()
    //获取上下文
    val sc: SparkContext = spark.sparkContext
    /*
    1:读取日志文件，解析封装为weblogbean对象
    2：过滤掉静态请求资源路径
    3：按照用户id分组，生成sessionid
    4：生成visit模型
     */
    //flume采集输出的路径
    val textRdd: RDD[String] = sc.textFile("/spark_etl/data/input2/")
    val webLogBeanRdd: RDD[WebLogBean] = textRdd.map(WebLogBean(_))
    //过滤掉不合法的请求
    val filterWeblogBeanRdd: RDD[WebLogBean] = webLogBeanRdd.filter(
      x => {
        x != null && x.valid
      }
    )
    //2：过滤掉静态请求资源路径,哪些是静态的资源路径，准备一个初始规则文件，初始的集合装有规则的路径
    initlizePages
    //使用广播变量广播规则
    val pagesBroadCast: Broadcast[mutable.HashSet[String]] = sc.broadcast(pages)
    val filterStaticWeblogRdd: RDD[WebLogBean] = filterWeblogBeanRdd.filter(
      bean => {
        val request: String = bean.request
        val res: Boolean = pagesBroadCast.value.contains(request) //pages直接如此使用是否好？是driver端还是executor端的？所以为了性能要考虑使用广播变量广播pages规则
        //如果被规则文件包含则过滤掉这个请求
        if (res) {
          false
        } else {
          true
        }
      }
    )

    //过滤和清洗已经完成

    import spark.implicits._
    val weblogBeanCaseDataset: Dataset[WeblogBeanCase] = filterStaticWeblogRdd.map(bean => WeblogBeanCase(bean.valid, bean.remote_addr, bean.remote_user, bean.time_local,
      bean.request, bean.status, bean.body_bytes_sent, bean.http_referer, bean.http_user_agent, bean.guid)).toDS()

    //保存为parquet文件
    weblogBeanCaseDataset.write.mode("overwrite")
      .parquet("/user/hive/warehouse/itcast_ods.db/weblog_origin/dt=20191101/")

//调用pageviewservice中方法来生成pageview模型数据
    PageViewService.savePageViewToHdfs(filterStaticWeblogRdd)




    //停止程序
    sc.stop()
  }

  //我们准备一个静态资源的集合
  // 用来存储网站url分类数据
  val pages = new mutable.HashSet[String]()

  //初始化静态资源路径集合
  def initlizePages(): Unit = {
    pages.add("/about")
    pages.add("/black-ip-list/")
    pages.add("/cassandra-clustor/")
    pages.add("/finance-rhive-repurchase/")
    pages.add("/hadoop-family-roadmap/")
    pages.add("/hadoop-hive-intro/")
    pages.add("/hadoop-zookeeper-intro/")
    pages.add("/hadoop-mahout-roadmap/")
  }
}
