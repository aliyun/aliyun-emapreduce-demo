package com.aliyun.emr.example.spark;

import com.alicloud.openservices.tablestore.ecosystem.ComputeParameters;
import com.alicloud.openservices.tablestore.ecosystem.Filter;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.tablestore.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

public class SparkTableStoreJavaDemo {
    private static RangeRowQueryCriteria fetchCriteria(String tableName, String columnName) {
        RangeRowQueryCriteria res = new RangeRowQueryCriteria(tableName);
        res.setMaxVersions(1);
        List<PrimaryKeyColumn> lower = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upper = new ArrayList<PrimaryKeyColumn>();
        lower.add(new PrimaryKeyColumn(columnName, PrimaryKeyValue.INF_MIN));
        upper.add(new PrimaryKeyColumn(columnName, PrimaryKeyValue.INF_MAX));
        res.setInclusiveStartPrimaryKey(new PrimaryKey(lower));
        res.setExclusiveEndPrimaryKey(new PrimaryKey(upper));
        return res;
    }

    public static void main(String[] args) {
        String accessKeyId = args[0];
        String accessKeySecret = args[1];
        Filter filter = new Filter(Filter.CompareOperator.GREATER_THAN,"PK", ColumnValue.fromLong(-1000));
        List<String> list = new ArrayList<>();
        list.add("VALUE");
        TableStoreFilterWritable tableStoreFilterWritable = new TableStoreFilterWritable(filter, list);

        String endpoint = args[2];
        String instance = args[3];
        String tableName = args[4];
        String primaryKeyColumnName = args[5];
        ComputeParams computeParams = new ComputeParams(100, 1, ComputeParameters.ComputeMode.Auto.name());
        SparkConf sparkConf = new SparkConf().setAppName("E-MapReduce Demo 5: Spark TableStore Demo (Java)");
        JavaSparkContext sc = null;
        try {
            sc = new JavaSparkContext(sparkConf);
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("computeParams", computeParams.serialize());
            hadoopConf.set("tableName", tableName);
            hadoopConf.set("filters", tableStoreFilterWritable.serialize());
            TableStore.setCredential(
                    hadoopConf,
                    new Credential(accessKeyId, accessKeySecret, null));
            Endpoint ep = new Endpoint(endpoint, instance);
            TableStore.setEndpoint(hadoopConf, ep);
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat.addCriteria(hadoopConf,
                    fetchCriteria(tableName, primaryKeyColumnName));
            JavaPairRDD<PrimaryKeyWritable, RowWritable> rdd = sc.newAPIHadoopRDD(
                    hadoopConf, com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat.class,
                    PrimaryKeyWritable.class, RowWritable.class);
            System.out.println(
                    new Formatter().format("TOTAL: %d", rdd.count()).toString());
            rdd.take(10).forEach((primaryKeyWritableRowWritableTuple2) -> {
                System.out.println(String.format("Key: %s, VALUE: %s",
                        primaryKeyWritableRowWritableTuple2._1.getPrimaryKey().toString(),
                        primaryKeyWritableRowWritableTuple2._2.getRow().toString()));
            });
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }
}
