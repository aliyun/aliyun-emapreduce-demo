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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SparkOssJavaDemo {

    public static void main(String[] args) throws Exception {

        String accessId = args[0];
        String accessKey = args[1];

        String endpoint = args[2];

        String inputPath = args[3];
        String outputPath = args[4];
        int partition = Integer.valueOf(args[5]);

        SparkConf sparkConf = new SparkConf().setAppName("E-MapReduce Demo 2-2: Spark Oss Demo (Java)").setMaster("local[4]");
        sparkConf.set("spark.hadoop.fs.oss.accessKeyId", accessId);
        sparkConf.set("spark.hadoop.fs.oss.accessKeySecret", accessKey);
        sparkConf.set("spark.hadoop.fs.oss.endpoint", endpoint);
        sparkConf.set("spark.hadoop.fs.oss.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem");
        sparkConf.set("spark.hadoop.mapreduce.job.run-local", "true");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaPairRDD<LongWritable, Text> data = jsc.hadoopFile(inputPath, TextInputFormat.class, LongWritable.class, Text.class, partition);

        System.out.println("Count (data): " + String.valueOf(data.count()));

        data.saveAsTextFile(outputPath);
    }

}
