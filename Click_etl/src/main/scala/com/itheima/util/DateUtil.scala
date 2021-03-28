package com.itheima.util

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtil {
  val DATE_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
  val DATE_KEY_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyyMMdd")
  val TIME_FORMAT1 = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss", Locale.US)
  val TIME_FORMAT2: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TIME_MINUTE_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmm")
  val GMT_FORMAT: FastDateFormat = FastDateFormat.getInstance("dd/MMM/yyyy hh:mm:ss", Locale.ENGLISH)


  /*
  计算两个日期之间的差值,毫秒值
   */
  def getTimeDiff(dateStr1: String, dateStr2: String): Long = {
    //得到的日期字符串是格式化好的
    TIME_FORMAT2.parse(dateStr2).getTime - TIME_FORMAT2.parse(dateStr1).getTime
  }


  //格式化时间方法
  def formatLogDate(time_local: String): String =
    try {
      val date = TIME_FORMAT1.parse(time_local)
      TIME_FORMAT2.format(date)
    }
    catch {
      case e: ParseException =>
        null
    }

  def main(args: Array[String]): Unit = {
    //    val msg = "222.66.59.174 - - [01/Nov/2018:06:53:30 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"" +
    //      "http://www.angularjs.cn/A00n\" " +
    //      "\"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0\"\n"
    //
    //    val arr = msg.split(" ")
    //    System.out.println(arr(3))
    //    val time_local = formatLogDate(arr(3).substring(1))
    //    System.out.println(time_local)
    var t1 = "2019-12-12 15:45:00"
    var t2 = "2019-12-12 16:45:00"

    val l = getTimeDiff(t1, t2)
    println(l)
  }

  /**
    * 获取当天日期 格式:yyyy-MM-dd
    *
    * @return 当天日期
    */
  def getTodayDate: String = {

    DATE_FORMAT.format(new Date)
  }

  /**
    * 获取昨天日期 格式:yyyy-MM-dd
    *
    * @return 昨天日期
    */
  def getYesterdayDate: String = {
    val cal = Calendar.getInstance
    cal.setTime(new Date)
    cal.add(Calendar.DATE, -1)
    DATE_FORMAT.format(cal.getTime)
  }

  /** 获取日期时间 格式:yyyy-MM-dd HH:mm:ss
    *
    * @param i :  当天为0，前一天为-1,后一天为1
    * @return yyyy-MM-dd  HH:mm:ss
    */
  def getDate(i: Int): String = {
    val cal = Calendar.getInstance
    cal.setTime(new Date)
    cal.add(Calendar.DATE, i)
    TIME_FORMAT1.format(cal.getTimeInMillis)
  }

  /**
    * 格式化日期 date转日期
    *
    * @param date Date对象 Sat Sep 07 03:02:01 CST 2019
    * @return yyyy-MM-dd
    */
  def formatDate(date: Date): String = DATE_FORMAT.format(date)


  /**
    * 格式化日期 string转日期
    *
    * @param date "yyyy-MM-dd HH:mm:ss"
    * @return yyyy-MM-dd
    */
  def formatDate(date: String): String = DATE_FORMAT.format(DATE_FORMAT.parse(date))

  /**
    * 格式化时间 date对象转时间
    *
    * @param date Date对象 Sat Sep 07 03:02:01 CST 2019
    * @return yyyy-MM-dd HH:mm:ss
    */
  def formatTime(date: Date): String = TIME_FORMAT1.format(date)


  /**
    * 格式化日期 date转yyyyMMdd日期
    *
    * @param date yyyy-MM-dd HH:mm:ss
    * @return yyyyMMdd
    */
  def formatKeyDate(date: Date): String = DATE_KEY_FORMAT.format(date)

  /**
    * 格式化日期 date string转yyyyMMdd日期,没有-间隔
    *
    * @param date "yyyy-MM-dd HH:mm:ss"
    * @return yyyyMMdd
    */
  def formatKeyDate(date: String): String = DATE_KEY_FORMAT.format(TIME_FORMAT1.parse(date))

  /**
    * 将GMT日期格式转换为时间戳
    *
    * @param gmt 07/Sep/2019:00:07:39 +0800
    * @return timestamp
    */
  def formatGmtToTimestamp(gmt: String): Long = GMT_FORMAT.parse(gmt).getTime

  /**
    * 格式化时间戳 date string转时间戳
    *
    * @param date "yyyy-MM-dd HH:mm:ss"
    * @return timestamp
    */
  def formatDateToTimestamp(date: String): Long = DATE_FORMAT.parse(date).getTime

  /**
    * 将CMT日期格式转换为时间
    *
    * @param gmt 07/Sep/2019:00:07:39 +0800
    * @return yyyy-MM-dd HH:mm:ss
    */
  def formatGmtToTime(gmt: String): String = TIME_FORMAT1.format(gmt)

  /**
    * 格式化时间 保留到分钟级
    *
    * @param date
    * @return yyyyMMddHHmm
    */
  def formatTimeMinute(date: Date): String = TIME_MINUTE_FORMAT.format(date)


  /**
    * 将时间转换为日期格式,格式化到月
    *
    * @param date yyyy-MM-dd HH:mm:ss
    * @return yyyy-MM
    */
  def formatDateToMonth(date: String): String = {
    val sdf = new SimpleDateFormat("yyyy-MM")
    sdf.format(TIME_FORMAT1.parse(date))
  }

  /**
    * 获取小时
    *
    * @param date yyyy-MM-dd HH:mm:ss
    * @return HH
    */
  def formatHour(date: String): String = {
    val sdf = new SimpleDateFormat("HH")
    sdf.format(TIME_FORMAT1.parse(date))
  }


}
