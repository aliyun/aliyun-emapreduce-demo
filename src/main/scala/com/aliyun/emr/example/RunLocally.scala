package com.aliyun.emr.example

import org.apache.spark.{SparkConf, SparkContext}

trait RunLocally {
  val conf = new SparkConf().setAppName(getAppName).setMaster("local[4]")
  conf.set("spark.hadoop.fs.oss.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")
  conf.set("spark.hadoop.job.runlocal", "true")
  val sc = new SparkContext(conf)

  def getAppName: String
}
