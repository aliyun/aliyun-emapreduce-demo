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

import com.aliyun.openservices.ons.api.Message
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.ons.OnsUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}
import scala.collection.JavaConversions._

object ConnectionUtil extends Serializable {
  private var conf: Configuration = null

  private var connection: Connection = null

  def getDefaultConn(quorum: String): Connection = {
    if (conf == null && connection == null) {
      conf = HBaseConfiguration.create()
      conf.set(HConstants.ZOOKEEPER_QUORUM, quorum)
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")
      connection = ConnectionFactory.createConnection(conf)
    }
    connection
  }
}

object HBaseSample {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        """Usage: bin/spark-submit --class HBaseSample examples-1.0-SNAPSHOT-shaded.jar <accessKeyId> <accessKeySecret>
          |         <consumerId> <topic> <subExpression> <parallelism> <tableName> <quorum>
          |
          |Arguments:
          |
          |    accessKeyId      Aliyun Access Key ID.
          |    accessKeySecret  Aliyun Key Secret.
          |    consumerId       ONS ConsumerID.
          |    topic            ONS topic.
          |    subExpression    * for all, or some specific tag.
          |    tableName        The name of HBase table.
          |    quorum           HBase quorum setting.
          |
        """.stripMargin)
      System.exit(1)
    }

    val Array(accessKeyId, accessKeySecret, consumerId, topic, subExpression, tname, quorum) = args

    val COLUMN_FAMILY_BYTES = Bytes.toBytes("count")
    val COLUMN_QUALIFIER_BYTES = Bytes.toBytes("count")

    val batchInterval = Seconds(2)

    val conf = new SparkConf().setAppName("Hbase Streaming Sample")
    val ssc = new StreamingContext(conf, batchInterval)
    def func: Message => Array[Byte] = msg => msg.getBody
    val onsStream = OnsUtils.createStream(
        ssc,
        consumerId,
        topic,
        subExpression,
        accessKeyId,
        accessKeySecret,
        StorageLevel.MEMORY_AND_DISK_2,
        func)

    onsStream.foreachRDD(rdd => {
      rdd.map(bytes => new String(bytes))
        .flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .mapPartitions {words => {
        val conn = ConnectionUtil.getDefaultConn(quorum)
        val tableName = TableName.valueOf(tname)
        val t = conn.getTable(tableName)
        try {
          words.sliding(100, 100).foreach(slice => {
            val puts = slice.map(word => {
              println(s"word: $word")
              val put = new Put(Bytes.toBytes(word._1 + System.currentTimeMillis()))
              put.addColumn(COLUMN_FAMILY_BYTES, COLUMN_QUALIFIER_BYTES,
                System.currentTimeMillis(), Bytes.toBytes(word._2))
              put
            }).toList
            t.put(puts)
          })
        } finally {
          t.close()
        }

        Iterator.empty
      }}.count()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
