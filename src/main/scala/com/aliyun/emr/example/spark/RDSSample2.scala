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

import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

object RDSSample2 extends RunLocally {
  def main(args: Array[String]): Unit = {
    if (args.length < 8) {
      System.err.println(
        """Usage: bin/spark-submit --class RDSSample1 examples-1.0-SNAPSHOT-shaded.jar <dbName> <tbName> <dbUser>
          |       <dbPwd> <dbUrl> <dbPort> <inputPath> <numPartitions>
          |
          |Arguments:
          |
          |    dbName        RDS database name.
          |    tbName        RDS table name.
          |    dbUser        RDS database user name.
          |    dbPwd         RDS database password.
          |    dbUrl         RDS database URL.
          |    dbPort        RDS database port
          |    inputPath     OSS input object path, like oss://accessKeyId:accessKeySecret@bucket.endpoint/a/b.txt
          |    numPartitions
          |
        """.stripMargin)
      System.exit(1)
    }
    val dbName = args(0)
    val tbName = args(1)
    val dbUser = args(2)
    val dbPwd = args(3)
    val dbUrl = args(4)
    val dbPort = args(5)
    val inputPath = args(6)
    val numPartitions = args(7).toInt

    val sqlContext = new SQLContext(getSparkContext)

    val input = getSparkContext.textFile(inputPath, numPartitions)
    val counts = input.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).map(e => Row.apply(e._1, e._2))

    lazy val schema = StructType(
        StructField("word", StringType) ::
        StructField("count", IntegerType) :: Nil)

    val properties = new Properties()
    properties.setProperty("user", dbUser)
    properties.setProperty("password", dbPwd)

    val df = sqlContext.createDataFrame(counts, schema)
    df.write.jdbc(s"jdbc:mysql://$dbUrl:$dbPort/$dbName", tbName, properties)
  }

  override def getAppName: String = "RDS Sample2"
}
