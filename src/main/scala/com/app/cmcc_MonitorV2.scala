package com.app

import com.utils.{AppUtils, JedisClusters, JedisOffsets}
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
      .set("spark.streaming.backpressure.enabled","true")
      //拉取数据的间隔时间
      .set("spark.streaming.kafka.maxRatePerPartition","10000")
      //处理完本批数据再停止
      .set("spark.streaming.kafka.stopGracefullyOnShutdown","true")

    //创建SparkStreaming
    val ssc = new StreamingContext(conf,Seconds(3))


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
    val fromOffset = JedisOffsets.apply(GROUP_ID,jedisD)

    //创建一个可变的数据流
    var stream: InputDStream[ConsumerRecord[String, String]] = null

    //判断  如果不是第一次读取数据  Offset应该大于0
    if (fromOffset.size > 0) {
      stream = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics,kafkaParams,fromOffset)
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
    val city: RDD[String] = ssc.sparkContext.textFile("Inpath/city.txt")
    city.foreach(println)
    val cityValue: collection.Map[String, String] = city.map(lines => {
      val str: Array[String] = lines.split(" ")
      (str(0), str(1))
    }).collectAsMap()
    //广播
    val cityBroadcast: Broadcast[collection.Map[String, String]] = ssc.sparkContext.broadcast(cityValue)

    //开始进行计算
    stream.foreachRDD(rdd=>{
      //为了后续更新Offset做准备
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //获取基本数据，保存到内存中，方便调用
      //      val baseData: RDD[(String, String, List[Double], String, String)] = AppUtils.Api_BaseDate(rdd)
      /**
        * 1.	业务概况---必做（结果存入Redis）
        * 1)	统计全网的充值订单量, 充值金额, 充值成功数、充值总时长
        * 2)	实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
        */
      //统计全网的充值订单量, 充值金额, 充值成功数、充值总时长
      AppUtils.Api_general_total(rdd)
      //      rdd.foreach(println)

      //更新偏移量
      for (o <- offsetRanges){
        //将Offs更新到Redis
        jedisD.hset(GROUP_ID,o.topic+"-"+o.partition,o.untilOffset.toString)
      }


    })

    //启动程序
    ssc.start()
    ssc.awaitTermination()

  }
}