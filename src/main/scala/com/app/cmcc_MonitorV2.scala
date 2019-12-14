package com.app

import com.utils.{JedisClusters, JedisOffsets, RechargeNoticeUtils, RechargeRequestUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object cmcc_MonitorV2 {

  def main(args: Array[String]): Unit = {
    //取消日志显示
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //配置Spark的属性
    val conf: SparkConf = new SparkConf()
      .setAppName("RealTimeRecharge")
      .setMaster("local[*]")
      //设置kryo序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //被压机制
      .set("spark.streaming.backpressure.enabled", "true")
      //拉取数据的间隔时间
      .set("spark.streaming.kafka.maxRatePerPartition", "10000")
      //处理完本批数据再停止
      .set("spark.streaming.kafka.stopGracefullyOnShutdown", "true")

    //创建SparkStreaming
    val ssc = new StreamingContext(conf, Seconds(3))

    //配置节点信息
    val BOOTSTRAP_SERVER = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    //消费者组
    val GROUP_ID = "real_consumer"
    //topic
    val topics = Array("realTimeRecharge")

    //配置Kafka的参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> BOOTSTRAP_SERVER,
      //Kafka需要配置解码器
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //配置消费者组
      "group.id" -> GROUP_ID,
      //设置从头消费
      "auto.offset.reset" -> "latest",
      //设置异步异步模式，一秒读取一次生产者数据
      //      "queue.buffering.max.ms" -> 1000,
      //设置是否自动提交offset（默认自动提交）
      "enable.auto.commit" -> "false"
    )

    val jedisD: Jedis = JedisClusters.JedisD
    val fromOffset = JedisOffsets.apply(GROUP_ID, jedisD)

    //创建一个可变的数据流
    var stream: InputDStream[ConsumerRecord[String, String]] = null

    //判断  如果不是第一次读取数据  Offset应该大于0
    if (fromOffset.size > 0) {
      stream = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics, kafkaParams, fromOffset)
      )
    } else {
      //如果不是第一次消费数据，那么从0开始读取
      stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
      println("第一次消费数据")
    }

    //获取省份的信息并且广播
    val city: RDD[String] = ssc.sparkContext.textFile("Input/city.txt")
    city.foreach(println)
    val cityValue: collection.Map[String, String] = city.map(lines => {
      val str: Array[String] = lines.split(" ")
      (str(0), str(1))
    }).collectAsMap()
    //广播
    val cityBroadcast: Broadcast[collection.Map[String, String]] = ssc.sparkContext.broadcast(cityValue)

    //开始进行计算
    stream.foreachRDD(rdd => {
      //为了后续更新Offset做准备
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      /**
        * 一、充值通知
        * 1.	业务概况---必做（结果存入Redis）
        * 1)	统计全网的充值订单量, 充值金额, 充值成功数、充值总时长
        * 2)	实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
        */
      //获取基本数据，保存到内存中，方便调用
      val noticeBaseData: RDD[(String, String, String, List[Double], String, String)] = RechargeNoticeUtils.api_baseDate(rdd)

      //统计全网的充值订单量, 充值金额, 充值成功数、充值总时长
      RechargeNoticeUtils.api_general_total(noticeBaseData)
      //实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
      RechargeNoticeUtils.api_general_hour(noticeBaseData)

      /**
        * 2.	业务质量（存入MySQL）
        * 2.1.	全国各省充值业务失败量分布
        * 统计每小时各个省份的充值失败数据量
        */
      //统计每小时各个省份的充值失败数据量
      RechargeNoticeUtils.api_general_province(noticeBaseData,cityBroadcast)

      /**
        * 3.	充值订单省份 TOP10（存入MySQL）
        * 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中，进行前台页面显示。
        */
      //以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中
      RechargeNoticeUtils.api_general_top10(noticeBaseData,cityBroadcast)

      /**
        * 4.	实时充值情况分布（存入MySQL）
        * 实时统计每小时的充值笔数和充值金额。
        */
      //实时统计每小时的充值笔数和充值金额
      RechargeNoticeUtils.api_realtime_hour(noticeBaseData)

      /**
        * 二、充值请求
        * 业务失败省份 TOP3（离线处理[每天]）（存入MySQL）
        * 以省份为维度统计每个省份的充值失败数,及失败率存入MySQL中。
        */
      //获取基本数据，保存到内存中，方便调用
      val requestBaseData: RDD[(String, String, String, List[Double], String, String)] = RechargeRequestUtils.baseData(rdd)
      //以省份为维度统计每个省份的充值失败数,及失败率存入MySQL中
      RechargeRequestUtils.failProvince(requestBaseData,cityBroadcast)

      /**
        * 1)	以省份为维度,统计每分钟各省的充值笔数和充值金额
        * 2)	以省份为维度,统计每小时各省的充值笔数和充值金额
        */
      //以省份为维度,统计每分钟各省的充值笔数和充值金额
      RechargeRequestUtils.minuteCount(requestBaseData,cityBroadcast)
      //以省份为维度,统计每小时各省的充值笔数和充值金额
      RechargeRequestUtils.hourCount(requestBaseData,cityBroadcast)

      //更新偏移量
      for (o <- offsetRanges) {
        //将Offs更新到Redis
        jedisD.hset(GROUP_ID, o.topic + "-" + o.partition, o.untilOffset.toString)
      }


    })

    //启动程序
    ssc.start()
    ssc.awaitTermination()

  }
}