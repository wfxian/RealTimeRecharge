package com.utils

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object AppUtils {

  /**
    * 解析文件  获取基础数据 并且保存到内存中
    *
    * @param rdd
    */
  def Api_BaseDate(rdd: RDD[ConsumerRecord[String, String]]) = {
    val baseData: RDD[(String, String, List[Double], String, String)] = rdd.map(r => JSON.parseObject(r.value()))
      .filter(x => x.getString("serviceName").equals("reChargeNotifyReq"))
      .map(m => {
        //取值
        //业务结果
        val bussinessRst: String = m.getString("bussinessRst")
        //充值金额
        val chargefee: Double = m.getString("chargefee").toDouble
        //省份编码
        val provinceCode: String = m.getString("provinceCode")
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
        //（年月日，小时，List（1，充值成功标志，充值金额，充值所花费的时间），省份编码，分钟）
        (date, hour, List[Double](1, succedResult._1, succedResult._2, succedResult._3), provinceCode, minute)
      }).cache()
    baseData
  }


  /**
    * 统计全网的充值订单量, 充值金额, 充值成功数、充值总时长
    * @param baseData
    */
  def Api_general_total(rdd: RDD[ConsumerRecord[String, String]]) = {
    rdd.map(r => JSON.parseObject(r.value()))
      .filter(x => x.getString("serviceName").equals("makeNewOrder"))
      .map(m => {
        //取值
        //订单号 -> 充值订单量
        val orderId: String = m.getString("orderId")
        //充值金额 -> 充值金额
        val chargefee: Double = m.getString("chargefee").toDouble
        //业务结果 -> 充值成功数  0000 则成功
        val bussinessRst: String = m.getString("bussinessRst")
        //获取充值发起的时间
        val startReqTime: String = m.getString("startReqTime")
        val date: String = startReqTime.substring(0, 8)
        //获取充值发起的结束时间
        val endReqTime: String = m.getString("endReqTime")
        //充值时长
        val time: Long = CalculateTools.getDate(startReqTime, endReqTime)
        val successResult: (Int, Double, Long) = if (bussinessRst.equals("0000")) {
          (1, chargefee, time)
        } else {
          (0, 0, 0)
        }
        //返回结果
        (date, List[Double](1, successResult._1, successResult._2, successResult._3), orderId)
      }).map(t => (t._1, t._2))
      .reduceByKey((list1, list2) => {
        list1.zip(list2)
          .map(x => x._1 + x._2)
      }).foreachPartition(f => {
      //保存到Redis
      val jedisD: Jedis = JedisClusters.JedisD
      f.foreach(i => {
        println(i._1)
        jedisD.hincrBy("A-" + i._1, "orderSum", i._2(0).toLong)
        jedisD.hincrBy("A-" + i._1, "successSum", i._2(1).toLong)
        jedisD.hincrBy("A-" + i._1, "moneySum", i._2(2).toLong)
        jedisD.hincrBy("A-" + i._1, "timeSum", i._2(3).toLong)
      })
    })
  }

  //  def Api_general_total(baseData: RDD[(String, String, List[Double], String, String)]) = {
  //    baseData.map(tp=>(tp._1,tp._3))
  //      .reduceByKey((list1,list2) => {
  //        list1.zip(list2)
  //          .map(x=>x._1+x._2)
  //      }).foreachPartition(f=>{
  //      //保存到Redis
  //      val jedisD: Jedis = JedisClusters.JedisD
  //      f.foreach(i=>{
  //        println(i._1)
  //        jedisD.
  //          hincrBy("A-" + i._1, "total", i._2(0).toLong)
  //        jedisD.
  //          hincrBy("A-" + i._1, "success", i._2(1).toLong)
  //        jedisD.
  //          hincrBy("A-" + i._1, "money", i._2(2).toLong)
  //        jedisD.
  //          hincrBy("A-" + i._1, "time", i._2(3).toLong)
  //      })
  //    })
  //  }



}
