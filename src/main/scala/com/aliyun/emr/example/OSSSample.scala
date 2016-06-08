/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.example

object OSSSample extends RunLocally {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        """Usage: bin/spark-submit --class OSSSample examples-1.0-SNAPSHOT-shaded.jar <inputPath> <numPartition>
          |
          |Arguments:
          |
          |    inputPath        Input OSS object path, like oss://accessKeyId:accessKeySecret@bucket.endpoint/a/b.txt
          |    numPartitions    the number of RDD partitions.
          |
        """.stripMargin)
      System.exit(1)
    }

    val inputPath = args(0)
    val numPartitions = args(1).toInt
    val ossData = sc.textFile(inputPath, numPartitions)
    println("The top 10 lines are:")
    ossData.top(10).foreach(println)
  }

  override def getAppName: String = "OSS Sample"
}
