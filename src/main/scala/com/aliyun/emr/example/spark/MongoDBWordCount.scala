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

import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._

import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MongoDBWordCount extends RunLocally {
  def main(args: Array[String]): Unit = {
    if (args.length < 12) {
      System.err.println(
        """Usage: bin/spark-submit --class MongoDBWordCount examples-1.0-SNAPSHOT-shaded.jar <dbName> <dbUrl> <dbPort>
          |         <userName> <pwd> <collectionName> <sampleRatio> <writeConcern> <splitSize> <splitKey> <inputPath>
          |         <numPartitions>
          |
          |Arguments:
          |
          |    dbName          MongoDB database name.
          |    dbUrl           MongoDB database URL.
          |    dbPort          MongoDB database port.
          |    userName        MongoDB database user name.
          |    pwd             mongoDB database password.
          |    collectionName  MongoDB collection name.
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
    val userName = args(3)
    val pwd = args(4)
    val collectionName = args(5)
    val sampleRatio = args(6).toFloat
    val writeConcern = args(7)
    val splitSize = args(8).toInt
    val splitKey = args(9)
    val inputPath = args(10)
    val numPartitions = args(11).toInt

    val sqlContext = new SQLContext(getSparkContext)

    val input = getSparkContext.textFile(inputPath, numPartitions)
    val counts = input.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).map(e => Row.apply(e._1, e._2))
    lazy val schema = StructType(
        StructField("word", StringType) ::
        StructField("count", IntegerType) :: Nil)

    val hosts = dbUrl.split(",").map(e => s"$e:$dbPort").toList
    val df = sqlContext.createDataFrame(counts, schema)
    val saveConfig = MongodbConfigBuilder(Map(Host -> hosts, Database -> dbName,
        Collection -> collectionName, SamplingRatio -> sampleRatio, WriteConcern -> writeConcern,
        SplitSize -> splitSize, SplitKey -> splitKey,
        Credentials -> List(com.stratio.datasource.mongodb.config.MongodbCredentials(userName, dbName, pwd.toCharArray))))
    df.saveToMongodb(saveConfig.build())
  }

  override def getAppName: String = "MongoDBWordCount"
}
