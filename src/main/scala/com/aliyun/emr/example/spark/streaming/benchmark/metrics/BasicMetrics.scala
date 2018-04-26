package com.aliyun.emr.example.spark.streaming.benchmark.metrics

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

class BasicMetrics extends Serializable {
  def getDuration(value: String, separator: String = ",") : Option[Long] = {
    val values = value.split(separator)
    if (values.length != 2) {
      println("invalid result when parse start-time and finish time, invalid pattern should be start-time,end-time. content:" + value)
      return None
    }
    val duration = values(1).toLong - values(0).toLong
    Some(duration)
  }

  def loadConfig(configFile: String): Properties = {
    val properties = new Properties()
    properties.load(new BufferedInputStream(new FileInputStream(configFile)))
    properties
  }
}
