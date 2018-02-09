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

/** Counts words in new text files created in the given directory */
object SparkWordCount extends RunLocally {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        """Usage: bin/spark-submit --class SparkWordCount examples-1.0-SNAPSHOT-shaded.jar <inputPath> <outputPath> <numPartition>
          |
          |Arguments:
          |
          |    inputPath        Input OSS object path, like oss://accessKeyId:accessKeySecret@bucket.endpoint/input/words.txt
          |    outputPath       Output OSS object path, like oss://accessKeyId:accessKeySecret@bucket.endpoint/output
          |    numPartitions    The number of RDD partitions.
          |
        """.stripMargin)
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt

    val input = getSparkContext.textFile(inputPath, numPartitions)
    val output = input.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)

    output.saveAsTextFile(outputPath)
  }

  override def getAppName: String = "SparkWordCount"
}
