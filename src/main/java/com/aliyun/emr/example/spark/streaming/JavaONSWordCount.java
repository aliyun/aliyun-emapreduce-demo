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

package com.aliyun.emr.example.spark.streaming;

import com.aliyun.openservices.ons.api.Message;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.aliyun.ons.OnsUtils;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Iterator;
import java.util.regex.Pattern;

public class JavaONSWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 5) {
            System.err.println("Usage: bin/spark-submit --class ONSSample " +
                "examples-1.0-SNAPSHOT-shaded.jar <accessKeyId> <accessKeySecret> " +
                "<consumerId> <topic> <subExpression>");
            System.exit(1);
        }

        String accessKeyId = args[0];
        String accessKeySecret = args[1];
        String consumerId = args[2];
        String topic = args[3];
        String subExpression = args[4];

        SparkConf sparkConf = new SparkConf().setAppName("JavaONSWordCount");
        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        JavaReceiverInputDStream<byte[]> lines = OnsUtils.createStream(jssc, consumerId, topic, subExpression,
                accessKeyId, accessKeySecret, StorageLevel.MEMORY_AND_DISK(), new Function<Message, byte[]>() {
                    @Override
                    public byte[] call(Message msg) throws Exception {
                        return msg.getBody();
                    }
                });

        JavaDStream<String> words = lines.map(new Function<byte[], String>() {
            @Override
            public String call(byte[] v1) throws Exception {
                return new String(v1);
            }
        }).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Lists.newArrayList(SPACE.split(x)).iterator();
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
