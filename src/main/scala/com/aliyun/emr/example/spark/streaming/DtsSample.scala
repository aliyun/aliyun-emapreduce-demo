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

package com.aliyun.emr.example.spark.streaming

import com.aliyun.drc.clusterclient.message.ClusterMessage

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.dts.DtsUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object DtsSample {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: DtsSample <accessKeyId> <accessKeySecret> <guid> <usePublicIp> <interval-mills>
                            |  <accessKeyId>      Aliyun Access Key ID.
                            |  <accessKeySecret>  Aliyun Access Key Secret.
                            |  <guid>             Aliyun DTS guid name.
                            |  <usePublicIp>      Use public Ip to access DTS or not.
                            |  <interval-mills>   The time interval at which streaming data will be divided into batches.
        """.stripMargin)
      System.exit(1)
    }

    val Array(accessKeyId, accessKeySecret, guid, usePublicIp, interval) = args
    val sparkConf = new SparkConf().setAppName("DtsSample")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Milliseconds(interval.toInt))

    def func: ClusterMessage => String = msg => msg.getRecord.toString

    val dtsStream = DtsUtils.createStream(
      ssc,
      accessKeyId,
      accessKeySecret,
      guid,
      func,
      StorageLevel.MEMORY_AND_DISK_2,
      usePublicIp.toBoolean)

    dtsStream.foreachRDD(rdd => {
      rdd.collect().foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
