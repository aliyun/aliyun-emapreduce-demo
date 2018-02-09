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

import java.sql.{Connection, DriverManager, PreparedStatement}

object RDSSample1 extends RunLocally {
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

    val input = getSparkContext.textFile(inputPath, numPartitions)
    input.collect().foreach(println)
    input.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
      .mapPartitions(e => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql = s"insert into $tbName(word, count) values (?, ?)"
        try {
          conn = DriverManager.getConnection(s"jdbc:mysql://$dbUrl:$dbPort/$dbName", dbUser, dbPwd)
          ps = conn.prepareStatement(sql)
          e.foreach(pair => {
            ps.setString(1, pair._1)
            ps.setLong(2, pair._2)
            ps.executeUpdate()
          })

          ps.close()
          conn.close()
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      Iterator.empty
    }).count()
  }

  override def getAppName: String = "RDS Sample1"
}
