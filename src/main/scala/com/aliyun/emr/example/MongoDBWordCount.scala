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

package com.aliyun.emr.example

import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._

import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class MongoDBWordCount extends RunLocally {
  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      System.err.println(
        """Usage: bin/spark-submit --class RDSSample1 examples-1.0-SNAPSHOT-shaded.jar <dbName> <dbUrl> <dbPort>
          |         <collectionName> <sampleRatio> <writeConcern> <splitSize> <splitKey> <inputPath> <numPartitions>
          |
          |Arguments:
          |
          |    dbName          MongoDB database name.
          |    dbUrl           MongoDB database URL.
          |    dbPort          MongoDB database port.
          |    collectionName  MongoDB collenction name.
          |    sampleRatio     MongoDB sample ratio.
          |    writeConcern    MongoDB write concern.
          |    splitSize       MongoDB split size.
          |    splitKey        MongoDB split key.
          |    inputPath       OSS input object path, like oss://accessKeyId:accessKeySecret@bucket.endpoint/a/b.txt
          |    numPartitions   RDD partition number.
          |
        """.stripMargin)
      System.exit(1)
    }

    val dbName = args(0)
    val dbUrl = args(1)
    val dbPort = args(2)
    val collectionName = args(3)
    val sampleRatio = args(4).toFloat
    val writeConcern = args(5)
    val splitSize = args(6).toInt
    val splitKey = args(7)
    val inputPath = args(8)
    val numPartitions = args(9).toInt

    val sqlContext = new SQLContext(sc)

    val input = sc.textFile(inputPath, numPartitions)
    val counts = input.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).map(e => Row.apply(e._1, e._2))
    lazy val schema = StructType(
        StructField("word", StringType) ::
        StructField("count", IntegerType) :: Nil)

    val df = sqlContext.createDataFrame(counts, schema)
    val saveConfig = MongodbConfigBuilder(Map(Host -> List(s"$dbUrl:$dbPort"), Database -> dbName,
        Collection -> collectionName, SamplingRatio -> sampleRatio, WriteConcern -> writeConcern,
        SplitSize -> splitSize, SplitKey -> splitKey))
    df.saveToMongodb(saveConfig.build())
  }

  override def getAppName: String = "MongoDBWordCount"
}
