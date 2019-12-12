package com.utils

import java.util

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

/**
  * 获取Offset
  */
object JedisOffsets {

  //  def apply(groupId:String,jedis:JedisCluster)={
  def apply(groupId:String,jedis:Jedis)={
    //创建Map
    var formOffset: Map[TopicPartition, Long] = Map[TopicPartition,Long]()

    //查询出每个topic下面的Partition
    val topicPartition: util.Map[String, String] = jedis.hgetAll(groupId)

    //将map转为List
    import scala.collection.JavaConversions._
    val topicPartitionOffset: List[(String, String)] = topicPartition.toList

    //循环输出
    for (topicPL <- topicPartitionOffset){
      //(topicPartition)，需要切分成topic 和 partition
      val topicAndPartition: Array[String] = topicPL._1.split("-")
      //offset
      val offset: String = topicPL._2
      //将数据存储到Map
      formOffset += (new TopicPartition(topicAndPartition(0),topicAndPartition(1).toInt) -> offset.toLong)
    }
    //    jedis.close()
    formOffset
  }
}