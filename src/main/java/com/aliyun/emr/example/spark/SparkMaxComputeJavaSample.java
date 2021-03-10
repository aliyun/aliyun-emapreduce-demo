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

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.aliyun.odps.OdpsOps;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.List;

public class SparkMaxComputeJavaSample {

    public static void main(String[] args) throws Exception {
        String partition = null;
        String accessId = args[0];
        String accessKey = args[1];

        String odpsUrl = args[2];

        String tunnelUrl = args[3];
        String project = args[4];
        String table = args[5];
        if (args.length > 6) {
            partition = args[6];
        }

        SparkConf sparkConf = new SparkConf().setAppName("E-MapReduce Demo 3-2: Spark MaxCompute Demo (Java)");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        OdpsOps odpsOps = new OdpsOps(jsc.sc(), accessId, accessKey, odpsUrl, tunnelUrl);

        System.out.println("Read odps table...");
        JavaRDD<List<Long>> readData = odpsOps.readTableWithJava(project, table, new RecordToLongs(), Integer.valueOf(partition));

        System.out.println("counts: ");
        System.out.println(readData.count());
    }

    static class RecordToLongs implements Function2<Record, TableSchema, List<Long>> {
        @Override
        public List<Long> call(Record record, TableSchema schema) throws Exception {
            List<Long> ret = new ArrayList<Long>();
            for (int i = 0; i < schema.getColumns().size(); i++) {
                ret.add(record.getBigint(i));
            }
            return ret;
        }
    }

}
