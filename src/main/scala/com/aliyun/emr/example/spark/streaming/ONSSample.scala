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

import java.util.{Properties, UUID}

import com.aliyun.openservices.ons.api.impl.ONSFactoryImpl
import com.aliyun.openservices.ons.api.{Message, PropertyKeyConst}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.ons.OnsUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ONSSample {
  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        """Usage: bin/spark-submit --class ONSSample examples-1.0-SNAPSHOT-shaded.jar <accessKeyId> <accessKeySecret>
          |         <consumerId> <topic> <subExpression> <parallelism>
          |
          |Arguments:
          |
          |    accessKeyId      Aliyun Access Key ID.
          |    accessKeySecret  Aliyun Key Secret.
          |    consumerId       ONS ConsumerID.
          |    topic            ONS topic.
          |    subExpression    * for all, or some specific tag.
          |    parallelism      The number of receivers.
          |
        """.stripMargin)
      System.exit(1)
    }

    val Array(accessKeyId, accessKeySecret, cId, topic, subExpression, parallelism) = args

    val numStreams = parallelism.toInt
    val batchInterval = Milliseconds(2000)

    val conf = new SparkConf().setAppName("ONS Sample")
    val ssc = new StreamingContext(conf, batchInterval)
    def func: Message => Array[Byte] = msg => msg.getBody
    val onsStreams = (0 until numStreams).map { i =>
      println(s"starting stream $i")
      OnsUtils.createStream(
        ssc,
        cId,
        topic,
        subExpression,
        accessKeyId,
        accessKeySecret,
        StorageLevel.MEMORY_AND_DISK_2,
        func)
    }

    val unionStreams = ssc.union(onsStreams)
    unionStreams.foreachRDD(rdd => println(s"count: ${rdd.count()}"))

    ssc.start()
    ssc.awaitTermination()
  }
}

object OnsRecordProducer {
  def main(args: Array[String]): Unit = {
    val Array(accessKeyId, accessKeySecret, pId, topic, tag, parallelism) = args

    val numPartition = parallelism.toInt
    val conf = new SparkConf().setAppName("ONS Record Producer")
    val sc = new SparkContext(conf)

    sc.parallelize(0 until numPartition, numPartition).mapPartitionsWithIndex {
      (index, itr) => {
        generate(index, accessKeyId, accessKeySecret, pId, topic, tag)
        Iterator.empty
      }
    }.count()
  }

  def generate(
                partitionId: Int,
                accessKeyId: String,
                accessKeySecret: String,
                pId: String,
                topic: String,
                tag: String): Unit = {
    val properties = new Properties()
    properties.put(PropertyKeyConst.ProducerId, pId)
    properties.put(PropertyKeyConst.AccessKey, accessKeyId)
    properties.put(PropertyKeyConst.SecretKey, accessKeySecret)
    val onsFactoryImpl = new ONSFactoryImpl
    val producer = onsFactoryImpl.createProducer(properties)
    producer.shutdown()
    producer.start()

    var count = 0
    while(true){
      val uuid = UUID.randomUUID()
      val msg = new Message(topic, tag, uuid.toString.getBytes)
      msg.setKey(s"ORDERID_${partitionId}_$count")
      producer.send(msg)
      count += 1
      Thread.sleep(100L)
    }
  }
}
