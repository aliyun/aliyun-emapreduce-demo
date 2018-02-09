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

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.mns.MnsUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MNSSample {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        """Usage: bin/spark-submit --class MNSSample examples-1.0-SNAPSHOT-shaded.jar <queueName> <accessKeyId> <accessKeySecret> <endpoint>""".stripMargin)
      System.exit(1)
    }
    val queueName = args(0)
    val accessKeyId = args(1)
    val accessKeySecret = args(2)
    val endpoint = args(3)

    val conf = new SparkConf().setAppName("MNS Sample")
    val batchInterval = Seconds(10)
    val ssc = new StreamingContext(conf, batchInterval)

    val mnsStream = MnsUtils.createPullingStreamAsBytes(ssc, queueName, accessKeyId, accessKeySecret, endpoint,
      StorageLevel.MEMORY_ONLY)
    mnsStream.foreachRDD( rdd => {
      rdd.collect().foreach(e => println(new String(e)))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
