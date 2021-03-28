package com.itheima.bean

case class WeblogBeanCase(valid: Boolean,
                          remote_addr: String,
                          remote_user: String,
                          time_local: String,
                          request: String,
                          status: String,
                          body_bytes_sent: String,
                          http_referer: String,
                          http_user_agent: String,
                          guid: String
                         )


/**
  * 对接外部数据的层，表结构定义最好跟外部数据源保持一致
  * 术语： 贴源表
  *
  * @author
  */
class WebLogBean  extends Serializable {

  var valid: Boolean = true // 判断数据是否合法
  var remote_addr: String = _ // 记录客户端的ip地址
  var remote_user: String = _ // 记录客户端用户名称,忽略属性"-"
  var time_local: String = _ // 记录访问时间与时区
  var request: String = _ // 记录请求的url与http协议
  var status: String = _ // 记录请求状态；成功是200
  var body_bytes_sent: String = _ // 记录发送给客户端文件主体内容大小
  var http_referer: String = _ // 用来记录从那个页面链接访问过来的
  var http_user_agent: String = _ // 记录客户浏览器的相关信息
  var guid: String = _ //用户id
  //重写tostring方法
  override def toString: String = {
    var sb = new StringBuilder();
    sb.append(this.valid);
    sb.append("\001").append(this.remote_addr);
    sb.append("\001").append(this.remote_user);
    sb.append("\001").append(this.time_local);
    sb.append("\001").append(this.request);
    sb.append("\001").append(this.status);
    sb.append("\001").append(this.body_bytes_sent);
    sb.append("\001").append(this.http_referer);
    sb.append("\001").append(this.http_user_agent);
    sb.append("\001").append(this.guid);
    sb.toString();
  }


}

//伴生对象
object WebLogBean {


  def main(args: Array[String]): Unit = {
    val msg = "093c52f5-dbbc-4bf7-83c9-679b29e8a803 190.186.221.69 - - 2018-11-02 21:38:18 " +
      "\"\\x80w\\x01\\x03\\x01\\x00N\\x00\\x00\\x00 \\x00\\x009\\x00\\x008\\x00\\x005\\x00\\x00\\x16\\x00\\x00\\x13\\x00\\x00\" " +
      "400 173 \"-\" \"-\""
    val bean = WebLogBean(msg)
    println(bean)
  }

  //使用apply方法解决对数据原始数据封装为bean对象
  def apply(line: String): WebLogBean = {
    val arr = line.split(" ")
    var webLogBean = new WebLogBean()
    try {

      if (arr.length > 11) {
        webLogBean.guid = arr(0)
        webLogBean.remote_addr = arr(1)
        webLogBean.remote_user = arr(2)
        //      var time_local = DateUtil.formatLogDate(arr(4).substring(1))
        var time_local = arr(4) + " " + arr(5)
        if (null == time_local || "" == time_local) time_local = "-invalid_time-"
        webLogBean.time_local = time_local
        webLogBean.request = arr(7)
        webLogBean.status = arr(9)
        webLogBean.body_bytes_sent = arr(10)
        webLogBean.http_referer = arr(11)
        //如果useragent元素较多，拼接useragent
        if (arr.length > 11) {
          val sb = new StringBuilder
          var i = 11
          while ( {
            i < arr.length
          }) {
            sb.append(arr(i))
              i += 1
          }
          webLogBean.http_user_agent = sb.toString
        }
        else {
          webLogBean.http_user_agent = arr(11)
        }

        if (webLogBean.status.toInt >= 400) { // 大于400，HTTP错误
          webLogBean.valid = false
        }

        if ("-invalid_time-" == webLogBean.time_local) webLogBean.valid = false
      } else {
        webLogBean = null
      }
    } catch {
      case _ => webLogBean = null
    }
    webLogBean
  }
}
