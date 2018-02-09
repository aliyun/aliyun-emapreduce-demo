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

public class ODPSJavaSample {

    public static void main(String[] args) throws Exception {

        String accessId = args[0];
        String accessKey = args[1];

        String odpsUrl = args[2];

        String tunnelUrl = args[3];
        String project = args[4];
        String table = args[5];
        String partition = args[6];

        SparkConf sparkConf = new SparkConf().setAppName("Spark ODPS Sample");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Integer> l = new ArrayList<Integer>();
        for (int i = 1; i <= 30; i++) l.add(i);
        JavaRDD<List<Long>> data = jsc.parallelize(l, 11).map(new Function<Integer, List<Long>>() {
            @Override
            public List<Long> call(Integer v1) throws Exception {
                int columns = 20;
                List<Long> ret = new ArrayList<Long>();
                for (int i = 0; i < columns; i++) ret.add((long) (v1 + i));
                return ret;
            }
        });

        OdpsOps odpsOps = new OdpsOps(jsc.sc(), accessId, accessKey, odpsUrl, tunnelUrl);

        System.out.println("Write odps table...");
        odpsOps.saveToTableWithJava(project, table, partition, data, new SaveRecord());

        System.out.println("Read odps table...");
        JavaRDD<List<Long>> readData = odpsOps.readTableWithJava(project, table, partition, new RecordToLongs(), 13);

        System.out.println("counts: " + readData.count());
    }

    static class RecordToLongs implements Function2<Record, TableSchema, List<Long>> {
        @Override
        public List<Long> call(Record record, TableSchema schema) throws Exception {
            List<Long> ret = new ArrayList<Long>();
            for (int i = 0; i < schema.getColumns().size(); i++) {
                ret.add(Long.valueOf(record.getString(i)));
            }
            return ret;
        }
    }

    static class SaveRecord implements Function3<List<Long>, Record, TableSchema, BoxedUnit> {
        @Override
        public BoxedUnit call(List<Long> data, Record record, TableSchema schema) throws Exception {
            for (int i = 0; i < schema.getColumns().size(); i++) {
                record.setString(i, data.get(i).toString());
            }
            return null;
        }
    }
}
