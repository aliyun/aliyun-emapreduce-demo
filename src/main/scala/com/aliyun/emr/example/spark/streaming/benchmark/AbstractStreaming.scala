package com.aliyun.emr.example.spark.streaming.benchmark

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


abstract class AbstractStreaming {
  var config: Properties= _

  def runJob(args: Array[String]): Unit = {
    config = loadConfig(args(0))
    val receiverCores = config.getProperty("partition.number").toInt / config.getProperty("kafka.partition.receiver.factor").toInt
    val executorCore = (config.getProperty("cluster.cores.total").toInt * config.getProperty("cpu.core.factor").toFloat - receiverCores).toInt/config.getProperty("spark.executor.instances").toInt
    val executorMem = config.getProperty("cluster.memory.per.node.mb").toInt * config.getProperty("cluster.worker.node.number").toInt / config.getProperty("spark.executor.instances").toInt
    val sparkConf = new SparkConf()
      .setAppName(config.getProperty("name"))
      .set("spark.yarn.am.memory.mb", config.getProperty("spark.yarn.am.memory.mb") + "m")
      .set("spark.yarn.am.cores", config.getProperty("spark.yarn.am.cores"))
      .set("spark.executor.instances", config.getProperty("spark.executor.instances"))
      .set("spark.executor.cores", executorCore.toString)
      .set("spark.executor.memory", executorMem + "m")
      .set("spark.streaming.blockInterval", config.getProperty("spark.streaming.blockInterval"))
    val ssc = new StreamingContext(new SparkContext(sparkConf), Duration(config.getProperty("duration.ms").toLong))

    val kafkaParam = Map[String, Object](
      "bootstrap.servers" -> config.getProperty("broker.list"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> config.getProperty("consumer.group"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(config.getProperty("topic")), kafkaParam))

    execute(stream)

    ssc.start()
    ssc.awaitTermination()
  }
  def execute(stream: InputDStream[ConsumerRecord[String, String]])

  def loadConfig(configFile: String): Properties = {
    val properties = new Properties()
    properties.load(new BufferedInputStream(new FileInputStream(configFile)))
    properties
  }

}
