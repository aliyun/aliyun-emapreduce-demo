/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.example.spark

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkConf

object SparkOssDemo extends RunLocally {
  var accessKeyId = ""
  var accessKeySecret = ""
  var endpoint = ""

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        """Usage: bin/spark-submit --class com.aliyun.emr.example.spark.SparkOssDemo examples-1.0-SNAPSHOT-shaded.jar
          |
          |Arguments:
          |
          |    accessKeyId      OSS accessKeyId
          |    accessKeySecret  OSS accessKeySecret
          |    endpoint         OSS endpoint
          |    inputPath        Input OSS object path, like oss://bucket/input/a.txt
          |    outputPath       Output OSS object path, like oss://bucket/output/
          |    numPartitions    the number of RDD partitions.
          |
        """.stripMargin)
      System.exit(1)
    }

    accessKeyId = args(0)
    accessKeySecret = args(1)
    endpoint = args(2)
    val inputPath = args(3)
    val outputPath = args(4)
    val numPartitions = args(5).toInt
    val ossData = getSparkContext.hadoopFile(inputPath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], numPartitions)
    print(ossData.count())

    ossData.saveAsTextFile(outputPath)
  }

  override def getAppName: String = "E-MapReduce Demo 2-1: Spark Oss Demo (Scala)"

  override def getSparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.hadoop.fs.oss.accessKeyId", accessKeyId)
    conf.set("spark.hadoop.fs.oss.accessKeySecret", accessKeySecret)
    conf.set("spark.hadoop.fs.oss.endpoint", endpoint)
  }
}
