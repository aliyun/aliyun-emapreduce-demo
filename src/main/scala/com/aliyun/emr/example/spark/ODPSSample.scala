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

import com.aliyun.odps.TableSchema
import com.aliyun.odps.data.Record
import org.apache.log4j.{Level, Logger}
import org.apache.spark.aliyun.odps.OdpsOps
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ODPSSample {
  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        """Usage: bin/spark-submit --class ODPSSample examples-1.0-SNAPSHOT-shaded.jar <accessKeyId> <accessKeySecret> <envType> <project> <table> <numPartitions>
          |
          |Arguments:
          |
          |    accessKeyId      Aliyun Access Key ID.
          |    accessKeySecret  Aliyun Key Secret.
          |    envType          0 or 1
          |                     0: Public environment, choose this type when local debug
          |                     1: Aliyun internal environment, choose this type when run in E-MapReduce.
          |    project          Aliyun ODPS project
          |    table            Aliyun ODPS table
          |    numPartitions    the number of RDD partitions
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)

    val accessKeyId = args(0)
    val accessKeySecret = args(1)
    val envType = args(2).toInt
    val project = args(3)
    val table = args(4)
    val numPartitions = args(5).toInt

    val urls = Seq(
      Seq("http://service.odps.aliyun.com/api", "http://dt.odps.aliyun.com"), // public environment
      Seq("http://odps-ext.aliyun-inc.com/api", "http://dt-ext.odps.aliyun-inc.com") // Aliyun internal environment
    )

    val conf = new SparkConf().setAppName("ODPS Sample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val odpsOps = envType match {
      case 0 =>
        OdpsOps(sc, accessKeyId, accessKeySecret, urls(0)(0), urls(0)(1))
      case 1 =>
        OdpsOps(sc, accessKeyId, accessKeySecret, urls(1)(0), urls(1)(1))
    }

    val sqlContext = new SQLContext(sc)
    val odpsData = odpsOps.loadOdpsTable(sqlContext, project, table, new Array[Int](0), numPartitions)
    odpsData.collect().foreach(println)
  }

  def read(record: Record, schema: TableSchema): String = {
    record.getString(0)
  }
}
