package com.utils

import java.sql.{Connection, DriverManager}

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * 充值通知工具类
  */
object RechargeNoticeUtils {

  /**
    * 解析文件  获取基础数据 并且保存到内存中
    *
    * @param rdd
    */
  def api_baseDate(rdd: RDD[ConsumerRecord[String, String]]) = {
    val baseData: RDD[(String, String, String, List[Double], String, String)] = rdd.map(r => JSON.parseObject(r.value()))
      .filter(x => x.getString("serviceName").equals("reChargeNotifyReq"))
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
        //获取充值的发起时间和结束时间
        val requestId: String = m.getString("requestId")
        //获取日期
        val date = requestId.substring(0, 8)
        //小时
        val hour = requestId.substring(8, 10)
        //分钟
        val minute = requestId.substring(10, 12)
        //充值结束的时间
        val receiveNotifyTime = m.getString("receiveNotifyTime")
        //充值所花费的时间
        val time = CalculateTools.getDate(requestId, receiveNotifyTime)
        //判断充值是否有效
        val succedResult: (Int, Double, Long) =
          if (bussinessRst.equals("0000")) {
            (1, chargefee, time)
          } else {
            (0, 0, 0)
          }
        //返回结果
        //（年月日，小时，分钟，List（1，充值成功标志，充值金额，充值所花费的时间），省份编码，订单号）
        (date, hour, minute, List[Double](1, succedResult._1, succedResult._2, succedResult._3), provinceCode, orderId)
      }).cache()
    baseData
  }


  /**
    * 统计全网的充值订单量, 充值金额, 充值成功数、充值总时长
    *
    * @param baseData （年月日，小时，分钟，List（1，充值成功标志，充值金额，充值所花费的时间），省份编码，订单号）
    */

  def api_general_total(baseData: RDD[(String, String, String, List[Double], String, String)]) = {
    //取到日期和List
    baseData.map(tp => (tp._1, tp._4))
      //对List进行reduce聚合
      .reduceByKey((list1, list2) => {
      list1.zip(list2)
        .map(x => x._1 + x._2)
    }).foreachPartition(f => {
      //保存到Redis
      val jedisD: Jedis = JedisClusters.JedisD
      f.foreach(i => {
        println(i._1)
        //充值总量
        jedisD.
          hincrBy("A-" + i._1, "total", i._2(0).toLong)
        //充值成功数
        jedisD.
          hincrBy("A-" + i._1, "success", i._2(1).toLong)
        //充值总金额
        jedisD.
          hincrBy("A-" + i._1, "money", i._2(2).toLong)
        //充值总时长
        jedisD.
          hincrBy("A-" + i._1, "time", i._2(3).toLong)
      })
      jedisD.close()
    })
  }

  //    def Api_general_total(rdd: RDD[ConsumerRecord[String, String]]) = {
  //      rdd.map(r => JSON.parseObject(r.value()))
  //        .filter(x => x.getString("serviceName").equals("reChargeNotifyReq"))
  //        .map(m => {
  //          //取值
  //          //订单号 -> 充值订单量
  //          val orderId: String = m.getString("orderId")
  //          //充值金额 -> 充值金额
  //          val chargefee: Double = m.getString("chargefee").toDouble
  //          //业务结果 -> 充值成功数  0000 则成功
  //          val bussinessRst: String = m.getString("bussinessRst")
  //          //获取充值的发起时间和结束时间
  //          val requestId: String = m.getString("requestId")
  //          //获取年月日
  //          val date = requestId.substring(0, 8)
  //          //小时
  //          val hour = requestId.substring(8, 10)
  //          //分钟
  //          val minute = requestId.substring(10, 12)
  //          //充值结束的时间
  //          val receiveNotifyTime = m.getString("receiveNotifyTime")
  //          //充值所花费的时间
  //          val time = CalculateTools.getDate(requestId, receiveNotifyTime)
  //          //判断是否充值成功
  //          val successResult: (Int, Double, Long) = if (bussinessRst.equals("0000")) {
  //            (1, chargefee, time)
  //          } else {
  //            (0, 0, 0)
  //          }
  //
  //          //返回结果
  //          (date, hour,minute,List[Double](1, successResult._1, successResult._2, successResult._3), orderId)
  //        }).map(t => (t._1, t._4))
  //        .reduceByKey((list1, list2) => {
  //          list1.zip(list2)
  //            .map(x => x._1 + x._2)
  //        }).foreachPartition(f => {
  //        //保存到Redis
  //        val jedisD: Jedis = JedisClusters.JedisD
  //        f.foreach(i => {
  //          println(i._1)
  //          jedisD.hincrBy("A-" + i._1, "orderSum", i._2(0).toLong)
  //          jedisD.hincrBy("A-" + i._1, "successSum", i._2(1).toLong)
  //          jedisD.hincrBy("A-" + i._1, "moneySum", i._2(2).toLong)
  //          jedisD.hincrBy("A-" + i._1, "timeSum", i._2(3).toLong)
  //        })
  //      })
  //    }


  /**
    * 实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
    *
    * @param baseData （年月日，小时，分钟，List（1，充值成功标志，充值金额，充值所花费的时间），省份编码，订单号）
    * @return
    */
  def api_general_hour(baseData: RDD[(String, String, String, List[Double], String, String)]) = {
    //取到年月日，分钟，List
    baseData.map(tp => ((tp._1, tp._3), tp._4))
      .reduceByKey((list1, list2) => {
        list1.zip(list2)
          .map(x => x._1 + x._2)
      }).foreachPartition(f => {
      val jedisD: Jedis = JedisClusters.JedisD
      f.foreach(i => {
        //B-年月日  total-分钟  结果
        //每分钟的订单总量
        jedisD
          .hincrBy("B-" + i._1._1, "total-" + i._1._2, i._2(0).toLong)
        //每分钟的成功订单数
        jedisD
          .hincrBy("B-" + i._1._1, "success-" + i._1._2, i._2(1).toLong)
      })
      jedisD.close()
    })
  }

  /**
    * 统计每小时各个省份的充值失败数据量
    *
    * @param baseData （年月日，小时，分钟，List（1，充值成功标志，充值金额，充值所花费的时间），省份编码，订单号）
    * @param cityBroadcast
    * @return
    */
  def api_general_province(baseData: RDD[(String, String, String, List[Double], String, String)],
                           cityBroadcast: Broadcast[collection.Map[String, String]]) = {
    //定义连接Mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/Spark"
    val userName = "root"
    val passWd = "123456"

    //取年月日，小时，省份，List
    baseData.map(tp => {
      //匹配省份
      val province: String = cityBroadcast.value.getOrElse(tp._5, "未知省份")
      //获取小时
      val hour: String = tp._2
      //获取年月日
      val date: String = tp._1
      //（（省份，小时），充值标志）
      var count: Int = 0
      if (tp._4(1).toInt == 0) {
        count = 1
      } else {
        count = 0
      }
      ((date, province, hour), count)
    })
      //聚合，按照省份和小时分组，获取总数
      .reduceByKey(_ + _)
      .foreachPartition(f => {
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = connection.prepareStatement("insert into api_general_province (date, province, hour, failCount) values (?,?,?,?)")
        //批次写入
        f.foreach(j => {
          //日期
          sql.setString(1, j._1._1)
          //省份，匹配广播数据
          sql.setString(2, j._1._2.toString)
          //小时
          sql.setString(3, j._1._3.toString)
          //失败数据量
          sql.setDouble(4, j._2.toDouble)

          val i = sql.executeUpdate()
          if (i > 0) println("写入成功") else println("写入失败")
        })
        sql.close()
        connection.close()
      })
  }

  /**
    * 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中，进行前台页面显示。
    *
    * @param baseData （年月日，小时，分钟，List（1，充值成功标志，充值金额，充值所花费的时间），省份编码，订单号）
    * @param cityBroadcast
    * @return
    */
  def api_general_top10(baseData: RDD[(String, String, String, List[Double], String, String)],
                        cityBroadcast: Broadcast[collection.Map[String, String]]) = {
    //定义连接Mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/Spark"
    val userName = "root"
    val passWd = "123456"

    //获取省份
    baseData.map(tp => (tp._5, tp._4))
      .reduceByKey((list1, list2) => {
        list1.zip(list2)
          .map(x => x._1 + x._2)
      })
      .sortBy(x => x._2(0), false)
      .foreachPartition(f => {
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = connection.prepareStatement("insert into api_general_top (province,total,failNum, successRate,rank) values (?,?,?,?,?)")
        var count = 1

        f.foreach(j => {
          if (count <= 10) {
            //省份，匹配广播数据
            sql.setString(1, cityBroadcast.value.getOrElse(j._1, "未知省份").toString)
            //总量
            sql.setDouble(2, j._2(0))
            //成功量
            sql.setDouble(3, j._2(1))
            //成功率
            sql.setDouble(4, j._2(1) / j._2(0).formatted("%.1f").toDouble)
            //排名
            sql.setInt(5, count)

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
    * 实时统计每小时的充值笔数和充值金额。
    *
    * @param baseData （年月日，小时，分钟，List（1，充值成功标志，充值金额，充值所花费的时间），省份编码，订单号）
    * @return
    */
  def api_realtime_hour(baseData: RDD[(String, String, String, List[Double], String, String)]) = {
    //定义连接Mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/Spark"
    val userName = "root"
    val passWd = "123456"

    //获取日期，小时，List
    baseData.map(tp => ((tp._1, tp._2), tp._4))
      .reduceByKey((list1, list2) => {
        list1.zip(list2)
          .map(x => x._1 + x._2)
      })
      .foreachPartition(f => {
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = connection.prepareStatement("insert into api_realtime_hour (date, hour, money, count) values (?,?,?,?)")

        f.foreach(j => {
          //日期
          sql.setString(1, j._1._1)
          //小时
          sql.setString(2, j._1._2)
          //总金额
          sql.setDouble(3, j._2(2))
          //充值笔数
          sql.setDouble(4, j._2(1))

          val i = sql.executeUpdate()
          if (i > 0) println("写入成功") else println("写入失败")
        })
        sql.close()
        connection.close()
      })
  }


}
