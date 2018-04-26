package com.aliyun.emr.example.spark.streaming.benchmark

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
object WordCount extends AbstractStreaming {
  override def execute(stream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    stream.flatMap(kv => {
      val value:List[(String, (Integer, Long))] = List()
      val eventTime = kv.key()
      for (v <- kv.value().split(" ")) {
        (v, (1, eventTime.toLong)) +: value
      }
      value
    }).reduceByKey((x,y) =>{
      val count = x._1 + y._1
      var eventTime = x._2
      if (x._2 < y._2) {
        eventTime = y._2
      }
      (count, eventTime)
    }).map(x => x._2._2).saveAsTextFiles(config.getProperty("filename.prefix") + config.getProperty("name") + "/result")
  }
}
