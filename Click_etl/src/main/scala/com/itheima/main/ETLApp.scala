package com.itheima.main

import java.util.UUID

import com.itheima.bean.{PageViewsBeanCase, VisitBeanCase, WebLogBean, WeblogBeanCase}
import com.itheima.util.DateUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ETLApp {
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
      .parquet("/apps/hive/warehouse/itcast_ods.db/weblog_origin/dt=20191101/")




    //pageview模型生成
    //按照用户id分组（uid,iterator(用户的所有访问记录)）
    val uidWeblogRdd: RDD[(String, Iterable[WebLogBean])] = filterStaticWeblogRdd.groupBy(x => x.guid)


    //排序
    val pageviewBeanCaseRdd: RDD[PageViewsBeanCase] = uidWeblogRdd.flatMap(
      item => {
        val uid = item._1
        //按照时间排序
        val sortWeblogBeanList: List[WebLogBean] = item._2.toList.sortBy(_.time_local)
        //两两比较，生成sessionid,step,staylong(停留时长),如何计算停留时长：遍历的时候都是计算上一条的停留时长，sessionid,step信息
        //只有一条记录：sessionid,step,staylong:60s
        val pageViewBeanCaseList: ListBuffer[PageViewsBeanCase] = new ListBuffer[PageViewsBeanCase]()
        import scala.util.control.Breaks._
        var sessionid: String = UUID.randomUUID().toString
        var step = 1
        var page_staylong = 60000
        breakable {
          for (num <- 0 until (sortWeblogBeanList.size)) {
            //一条访问记录
            val bean: WebLogBean = sortWeblogBeanList(num)
            //如果只由一条记录
            if (sortWeblogBeanList.size == 1) {
              val pageViewsBeanCase = PageViewsBeanCase(sessionid, bean.remote_addr, bean.time_local,
                bean.request, step, page_staylong,
                bean.http_referer, bean.http_user_agent, bean.body_bytes_sent, bean.status, bean.guid)
              //之前输出，现在则需要保存起来最后一起输出
              pageViewBeanCaseList += pageViewsBeanCase
              //重新生成sessionid
              sessionid = UUID.randomUUID().toString
              //跳出循环
              break
            }
            //数量不止一条，本条来计算上一条的时长
            breakable {
              if (num == 0) {
                //continue:中止本次，进入下次循环
                break()
              }
              //不是第一条数据，获取到上一条记录
              val lastBean: WebLogBean = sortWeblogBeanList(num - 1)
              //判断的是两个bean对象的差值
              val timeDiff: Long = DateUtil.getTimeDiff(lastBean.time_local, bean.time_local)
              //毫秒值是否小于30分钟
              if (timeDiff <= 30 * 60 * 1000) {
                //属于同个session，你们俩共用一个sessionid,输出上一条
                val pageViewsBeanCase = PageViewsBeanCase(sessionid, lastBean.remote_addr, lastBean.time_local, lastBean.request,
                  step, timeDiff, lastBean.http_referer, lastBean.http_user_agent, lastBean.body_bytes_sent,
                  lastBean.status, lastBean.guid
                )
                //添加到集合中
                pageViewBeanCaseList += pageViewsBeanCase
                //sessionid是否重新生成：不需要，step如何处理？
                step += 1

              } else {
                //不属于一个sessionid,如何处理？不管是否属于同个会话，我们输出的都是上一条记录
                //属于同个session，你们俩共用一个sessionid,输出上一条
                val pageViewsBeanCase = PageViewsBeanCase(sessionid, lastBean.remote_addr, lastBean.time_local, lastBean.request,
                  step, page_staylong, lastBean.http_referer,
                  lastBean.http_user_agent, lastBean.body_bytes_sent,
                  lastBean.status, lastBean.guid
                )
                //添加到集合中
                pageViewBeanCaseList += pageViewsBeanCase
                //sessionid是否重新生成：需要，step如何处理？
                sessionid = UUID.randomUUID().toString
                //step重置为1
                step = 1
              }

              //最后一条需要我们控制输出
              if (num == sortWeblogBeanList.size - 1) {

                val pageViewsBeanCase = PageViewsBeanCase(sessionid, bean.remote_addr, bean.time_local,
                  bean.request, step, page_staylong,
                  bean.http_referer, bean.http_user_agent, bean.body_bytes_sent, bean.status, bean.guid)
                //之前输出，现在则需要保存起来最后一起输出
                pageViewBeanCaseList += pageViewsBeanCase
                //重新生成sessionid
                sessionid = UUID.randomUUID().toString
              }
            }

          }
        }

        pageViewBeanCaseList
      }
    )

    pageviewBeanCaseRdd.toDS().write.mode("overwrite")
      .parquet("/apps/hive/warehouse/itcast_ods.db/click_pageviews/dt=20191101/")


    //生成visit模型，汇总每个会话的总步长，时长等信息
    //按照sessionid分组,(sessionid,iterable(pageviewbeancase))
    val sessionidRdd: RDD[(String, Iterable[PageViewsBeanCase])] = pageviewBeanCaseRdd.groupBy(_.session)

    val visitBeanCaseRdd: RDD[VisitBeanCase] = sessionidRdd.map(
      item => {
        //sessionid
        val sessionid: String = item._1
        //sessionid对应的访问记录
        val cases: List[PageViewsBeanCase] = item._2.toList.sortBy(_.time_local)
        VisitBeanCase(cases(0).guid, sessionid, cases(0).remote_addr, cases(0).time_local, cases(cases.size - 1).time_local,
          cases(0).request, cases(cases.size - 1).request, cases(0).htp_referer, cases.size
        )
      }
    )

    visitBeanCaseRdd.toDS().write.mode("overwrite")
      .parquet("/apps/hive/warehouse/itcast_ods.db/click_stream_visit/dt=20191101/")


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
