package com.msb.stream.util

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

/**
  * 日期工具类
  */
object DateUtils {

  def getMin(date: Date): String = {
    val pattern = "yyyyMMddHHmm"
    val dateFormat = new SimpleDateFormat(pattern)
    val min = dateFormat.format(date)
    min
  }

}
