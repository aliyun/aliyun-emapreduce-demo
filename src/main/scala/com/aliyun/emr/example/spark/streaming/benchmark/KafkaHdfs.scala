package com.aliyun.emr.example.spark.streaming.benchmark

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream

object KafkaHdfs extends AbstractStreaming {
  override def execute(stream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    stream.map(kv => kv.key() + "," + System.currentTimeMillis())
      .saveAsTextFiles(config.getProperty("filename.prefix") + config.getProperty("name") + "/result")
  }

  def main(args: Array[String]): Unit = {
    runJob(args)
  }
}



