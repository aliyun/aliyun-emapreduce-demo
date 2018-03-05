/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.emr.examples.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HTableExample {


    public static void main(String[] args) throws Exception {

        Configuration configuration = HBaseConfiguration.create();

        // hbase-site.xml可以从E-MapReduce集群/etc/ecm/hbase-conf下获取
        String hbaseConfigPath = "file:///etc/ecm/hbase-conf/hbase-site.xml";
        configuration.addResource(new Path(hbaseConfigPath));
        configuration.set("hadoop.security.authentication","Kerberos");

        /*
         * 访问Kerberos的HBase集群，需要添加的login代码
         * 详见如何产生principal/keytab文件:
         * https://help.aliyun.com/document_detail/62966.html
         */
        UserGroupInformation.setConfiguration(configuration);
        String principal = "test";
        String keytab = "/root/test.keytab";
        UserGroupInformation.loginUserFromKeytab(principal, keytab);

        TableName tableName = TableName.valueOf("emr-test-hbase");
        // java 1.7 try-with-resources用法, 会自动调用conn.close()
        try (final Connection conn = ConnectionFactory.createConnection()) {
            System.out.println("***** create table " + tableName.getNameAsString() + " *****");
            createTable(conn, tableName);

            System.out.println("***** put data to table " + tableName.getNameAsString() + " *****");
            putData(conn, tableName);

            System.out.println("***** list put data to table " + tableName.getNameAsString() + " *****");
            listPutsData(conn, tableName);

            System.out.println("***** get data from table " + tableName.getNameAsString() + " *****");
            getData(conn, tableName);

            System.out.println("***** scan data from table " + tableName.getNameAsString() + " *****");
            scanData(conn, tableName);

            System.out.println("***** delete data from table " + tableName.getNameAsString() + " *****");
            deleteData(conn, tableName);

            System.out.println("***** disable and drop table " + tableName.getNameAsString() + " *****");
            dropTable(conn, tableName);
        }
    }

    private static void createTable(Connection conn, TableName tableName) throws IOException {
        // java 1.7 try-with-resources用法, 会自动调用admin.close()
        try (final Admin admin = conn.getAdmin()) {
            if (admin.tableExists(tableName)) {
                System.out.println("Table already exists:" + tableName.getNameAsString());
            } else {
                HTableDescriptor descriptor = new HTableDescriptor(tableName);
                descriptor.addFamily(new HColumnDescriptor("cf"));
                admin.createTable(descriptor);
            }
        }
    }

    private static void dropTable(Connection conn, TableName tableName) throws IOException {
        // java 1.7 try-with-resources用法, 会自动调用admin.close()
        try (final Admin admin = conn.getAdmin()) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            } else {
                System.out.println("Table " + tableName.getNameAsString() + "does not exists.");
            }
        }
    }

    private static void putData(Connection conn, TableName tableName) throws IOException {
        try (final HTable htable = (HTable) conn.getTable(tableName)) {
            Put p = new Put(Bytes.toBytes("row-1"));
            byte[] family = Bytes.toBytes("cf");
            byte[] qualifier = Bytes.toBytes("c1");

            p.addColumn(family, qualifier, Bytes.toBytes("V-01"));

            // 默认是true, 可设置为false
            // htable.setAutoFlushTo(false);
            htable.put(p);
        }
    }

    private static void listPutsData(Connection conn, TableName tableName) throws IOException {
        try (final HTable htable = (HTable) conn.getTable(tableName)) {
            List<Put> puts = new ArrayList<Put>();
            byte[] family = Bytes.toBytes("cf");

            Put p1 = new Put(Bytes.toBytes("row-1"));
            byte[] qualifier1 = Bytes.toBytes("c1");
            p1.addColumn(family, qualifier1, Bytes.toBytes("V-01"));
            puts.add(p1);

            Put p2 = new Put(Bytes.toBytes("row-1"));
            byte[] qualifier2 = Bytes.toBytes("c2");
            p2.addColumn(family, qualifier2, Bytes.toBytes("V-02"));
            puts.add(p2);

            Put p3 = new Put(Bytes.toBytes("row-2"));
            p3.addColumn(family, qualifier1, Bytes.toBytes("V-03"));
            puts.add(p3);

            htable.setAutoFlushTo(false);
            htable.put(puts);
            htable.flushCommits();
        }
    }

    private static void getData(Connection conn, TableName tableName) throws IOException {
        try (final Table table = conn.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes("row-1"));
            Result result = table.get(get);

            for (Cell cell : result.rawCells()) {
                System.out.println("row name:" + new String(CellUtil.cloneRow(cell)));
                System.out.println("timestamp:" + cell.getTimestamp());
                System.out.println("column family:" + new String(CellUtil.cloneFamily(cell)));
                System.out.println("column name:" + new String(CellUtil.cloneQualifier(cell)));
                System.out.println("value:" + new String(CellUtil.cloneValue(cell)));
                System.out.println("\n");
            }
        }
    }

    private static void scanData(Connection conn, TableName tableName) throws IOException {
        try (final Table table = conn.getTable(tableName)) {
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);

            for (Result result : resultScanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("row:" + new String(CellUtil.cloneRow(cell)));
                    System.out.println("timestamp:" + cell.getTimestamp());
                    System.out.println("column family:" + new String(CellUtil.cloneFamily(cell)));
                    System.out.println("column name:" + new String(CellUtil.cloneQualifier(cell)));
                    System.out.println("value:" + new String(CellUtil.cloneValue(cell)));
                    System.out.println("\n");
                }
            }
        }
    }

    private static void deleteData(Connection conn, TableName tableName) throws IOException {
        try (final Table table = conn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes("row-1"));
            table.delete(delete);
        }
    }
}
