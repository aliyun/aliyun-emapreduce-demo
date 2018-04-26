package com.aliyun.emr.example.spark.streaming.benchmark.metrics

import org.apache.spark.{SparkConf, SparkContext}

object HdfsMetrics extends BasicMetrics {
  private final val AppName = "Metrics"
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        """Usage: bin/spark-submit --class com.aliyun.emr.example.spark.streaming.benchmark.HdfsMetrics examples-1.1-shaded.jar <configFilePath>
          |
          |Arguments:
          |
          |    configFilePath   config file path, like benchmark.properties
          |
        """.stripMargin)
      System.exit(1)
    }

    val config = loadConfig(args(0))

    val conf = new SparkConf()
    conf.setAppName(AppName)

    var inputPath : String = null
    if (!config.getProperty("from.spark.streaming").toBoolean) {
      inputPath = config.getProperty("filename.prefix") + config.getProperty("benchmark.app.name") + "/*.txt"
    } else {
      inputPath = config.getProperty("filename.prefix") + config.getProperty("benchmark.app.name") + "/*/part-*"

    }
    val input = new SparkContext(conf).textFile(inputPath, config.getProperty("metric.numPartitions").toInt)
    val output = input.map(x => getDuration(x))
      .filter(x => x.isDefined)
      .map(x => x.get)

    val count = output.count()
    println("total:%d".format(count))
    output.histogram(Array(Double.MinValue, 0.0, 300.0, 500.0, 800.0, 900.0, 1000.0, 2000.0, 3000.0, Double.MaxValue)).foreach(x=> println(x.toDouble / count))
  }
}

