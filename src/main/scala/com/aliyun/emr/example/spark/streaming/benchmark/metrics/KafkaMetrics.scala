package com.aliyun.emr.example.spark.streaming.benchmark.metrics

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

object KafkaMetrics extends BasicMetrics {
  def main(args: Array[String]): Unit = {

    val config = loadConfig(args(0))

    val ssc = new StreamingContext(new SparkConf().setAppName("KafkaMetrics"), Seconds(config.getProperty("metric.duration.second").toLong))
    val kafkaParam = Map[String, Object] (
      "bootstrap.servers" -> config.getProperty("result.broker.list"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> config.getProperty("metric.group.id"),
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)

    )
    val messages  = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config.getProperty("result.topic")), kafkaParam))

    val outputPath = config.getProperty("filename.prefix") + config.getProperty("benchmark.app.name") + "/kafka-"
    messages.map(_.value()).saveAsTextFiles(outputPath)

    ssc.start()
    ssc.awaitTermination()
  }
}
