package com.utils

import java.sql.{Connection, DriverManager}

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * 充值请求工具类
  */
object RechargeRequestUtils {

  /**
    * 解析文件  获取基础数据 并且保存到内存中
    *
    * @param rdd
    */
  def baseData(rdd: RDD[ConsumerRecord[String, String]]) = {
    val baseData: RDD[(String, String, String, List[Double], String, String)] = rdd.map(r => JSON.parseObject(r.value()))
      .filter(x => x.getString("serviceName").equals("sendRechargeReq"))
      .map(m => {
        //取值
        //业务结果
        val bussinessRst: String = m.getString("bussinessRst")
        //充值金额
        val chargefee: Double = m.getString("chargefee").toDouble
        //省份编码
        val provinceCode: String = m.getString("provinceCode")
        //订单号 -> 充值订单量
        val orderId: String = m.getString("orderId")
        //业务流水号
        val requestId: String = m.getString("requestId")
        //获取日期
        val date = requestId.substring(0, 8)
        //小时
        val hour = requestId.substring(8, 10)
        //分钟
        val minute = requestId.substring(10, 12)
        //判断充值是否有效
        val successResult: (Int, Double) = if (bussinessRst.equals("0000")) {
          (1, chargefee)
        } else {
          (0, 0)
        }

        //返回结果
        //（日期，小时，分钟，List（1，充值成功标志，充值金额），省份，订单号）
        (date, hour, minute, List[Double](1, successResult._1, successResult._2), provinceCode, orderId)
      }).cache()
    baseData
  }

  /**
    * 以省份为维度统计每个省份的充值失败数,及失败率存入MySQL中。
    *
    * @param requestBaseData （日期，小时，分钟，List（1，充值成功标志，充值金额），省份，订单号）
    * @param cityBroadcast
    * @return
    */
  def failProvince(requestBaseData: RDD[(String, String, String, List[Double], String, String)],
                   cityBroadcast: Broadcast[collection.Map[String, String]]) = {
    //定义连接Mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/Spark"
    val userName = "root"
    val passWd = "123456"

    requestBaseData.map(tp => (tp._5, tp._4))
      .reduceByKey((list1, list2) => {
        list1.zip(list2)
          .map(x => x._1 + x._2)
      }).foreachPartition(f => {
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      val sql = connection.prepareStatement("insert into api_failProvince (province, failCount) values (?,?)")
      var count = 1
      f.foreach(j => {
        if (count <= 3) {
          //省份
          sql.setString(1, cityBroadcast.value.getOrElse(j._1, "未知省份"))
          //失败次数
          sql.setInt(2, (j._2(0) - j._2(1)).toInt)

          val i = sql.executeUpdate()
          if (i > 0) println("写入成功") else println("写入失败")
          count += 1
        }
      })
      sql.close()
      connection.close()
    })
  }


  /**
    * 以省份为维度,统计每分钟各省的充值笔数和充值金额
    *
    * @param requestBaseData （日期，小时，分钟，List（1，充值成功标志，充值金额），省份，订单号）
    * @param cityBroadcast
    * @return
    */
  def minuteCount(requestBaseData: RDD[(String, String, String, List[Double], String, String)],
                  cityBroadcast: Broadcast[collection.Map[String, String]]) = {
    //定义连接Mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/Spark"
    val userName = "root"
    val passWd = "123456"

    requestBaseData.map(tp => ((tp._5, tp._1, tp._2, tp._3), tp._4))
      .reduceByKey((list1, list2) => {
        list1.zip(list2)
          .map(x => x._1 + x._2)
      }).foreachPartition(f => {
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      val sql = connection.prepareStatement("insert into api_minuteCount (province, date, Count, money) values (?,?,?,?)")
      f.foreach(j => {
        sql.setString(1, cityBroadcast.value.getOrElse(j._1._1, "未知省份"))
        sql.setString(2, j._1._2 + "-" + j._1._3 + "-" + j._1._4)
        sql.setInt(3, j._2(1).toInt)
        sql.setDouble(4, j._2(2))

        val i = sql.executeUpdate()
        if (i > 0) println("写入成功") else println("写入失败")
      })
      sql.close()
      connection.close()
    })
  }


  /**
    * 以省份为维度,统计每小时各省的充值笔数和充值金额
    *
    * @param requestBaseData （日期，小时，分钟，List（1，充值成功标志，充值金额），省份，订单号）
    * @param cityBroadcast
    * @return
    */
  def hourCount(requestBaseData: RDD[(String, String, String, List[Double], String, String)],
                cityBroadcast: Broadcast[collection.Map[String, String]]) = {
    //定义连接Mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/Spark"
    val userName = "root"
    val passWd = "123456"

    requestBaseData.map(tp => ((tp._5, tp._1, tp._2), tp._4))
      .reduceByKey((list1, list2) => {
        list1.zip(list2)
          .map(x => x._1 + x._2)
      }).foreachPartition(f => {
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      val sql = connection.prepareStatement("insert into api_hourCount (province, date, Count, money) values (?,?,?,?)")
      f.foreach(j => {
        sql.setString(1, cityBroadcast.value.getOrElse(j._1._1, "未知省份"))
        sql.setString(2, j._1._2 + "-" + j._1._3)
        sql.setInt(3, j._2(1).toInt)
        sql.setDouble(4, j._2(2))

        val i = sql.executeUpdate()
        if (i > 0) println("写入成功") else println("写入失败")
      })
      sql.close()
      connection.close()
    })
  }
}
