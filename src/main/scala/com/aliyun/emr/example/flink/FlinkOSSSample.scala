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

package com.aliyun.emr.example.flink

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.JavaConversions._

object FlinkOSSSample {
  def main(args: Array[String]) {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    if (!params.has("input")) {
      println("Executing WordCount example with default input data set.")
      println("Use --input to specify file input.")
      sys.exit(1)
    }
    val text = env.readTextFile(params.get("input"))

    val top10 = text.first(10)

    top10.collect().foreach(println)

  }
}
