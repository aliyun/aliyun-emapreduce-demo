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

package com.aliyun.emr.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

public class JavaLoghubWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws InterruptedException {
    if (args.length < 6) {
      System.err.println("Usage: bin/spark-submit --class JavaLoghubWordCount " +
          "examples-1.0-SNAPSHOT-shaded.jar <sls project> <sls logstore> <loghub group name> " +
          "<sls endpoint> <access key id> <access key secret>");
      System.exit(1);
    }

    String loghubProject = args[0];
    String logStore = args[1];
    String loghubGroupName = args[2];
    String endpoint = args[3];
    String accessKeyId = args[4];
    String accessKeySecret = args[5];

    SparkConf conf = new SparkConf().setAppName("Loghub Sample");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(2000));
    JavaDStream<byte[]> lines = LoghubUtils.createStream(
      jssc,
      loghubProject,
      logStore,
      loghubGroupName,
      endpoint,
      1,
      accessKeyId,
      accessKeySecret,
      StorageLevel.MEMORY_AND_DISK());

    JavaDStream<String> words = lines.map(new Function<byte[], String>() {
      @Override
      public String call(byte[] v1) throws Exception {
        return new String(v1);
      }
    }).flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) {
        return Arrays.asList(SPACE.split(s)).iterator();
      }
    });
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    });

    wordCounts.print();
    jssc.start();
    jssc.awaitTermination();
  }
}
