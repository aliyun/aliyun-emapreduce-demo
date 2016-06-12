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

package com.aliyun.emr.example.streaming

import com.aliyun.emr.example.RunLocally
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import com.redislabs.provider.redis._

object RedisWordCount extends RunLocally {
  var redisHost = ""
  var redisPort = ""
  var redisAuth = ""

  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      System.err.println(
        """Usage: bin/spark-submit --class RedisWordCount examples-1.0-SNAPSHOT-shaded.jar <redisHost> <redisPort>
          |           <redisAuth> <keyName>
          |
          |Arguments:
          |
          |    redisHost       Redis host.
          |    redisPort       Redis port.
          |    redisAuth       Redis auth.
          |    keyName         Redis key name.
          |
        """.stripMargin)
      System.exit(1)
    }

    RedisWordCount.redisHost = args(0)
    RedisWordCount.redisPort = args(1)
    RedisWordCount.redisAuth = args(2)
    val keyName = args(3)

    val ssc = new StreamingContext(sc, Seconds(1))
    val redisStream = ssc.createRedisStream(Array(keyName), storageLevel = StorageLevel.MEMORY_AND_DISK_2)
    redisStream.print()
    ssc.awaitTermination()
  }

  override def getAppName: String = "Redis WordCount"

  override def initializeConf(conf: SparkConf): Unit = {
    conf.set("redis.host", RedisWordCount.redisHost)
    conf.set("redis.port", RedisWordCount.redisPort)
    conf.set("redis.auth", RedisWordCount.redisAuth)
  }
}
