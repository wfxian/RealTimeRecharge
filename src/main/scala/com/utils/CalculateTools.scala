package com.utils

import java.text.SimpleDateFormat

/**
  * 计算时间
  */
object CalculateTools {
  def getDate(startReqTime: String, endReqTime: String):Long = {
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    format.parse(endReqTime).getTime - format.parse(startReqTime).getTime
  }

}