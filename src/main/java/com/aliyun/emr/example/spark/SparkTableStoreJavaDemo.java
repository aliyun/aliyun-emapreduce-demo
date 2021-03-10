package com.aliyun.emr.example.spark;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.aliyun.openservices.tablestore.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;

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
        String securityToken = args[2];
        String endpoint = args[3];
        String instance = args[4];
        String tableName = args[5];
        String primaryKeyColumnName = args[6];
        SparkConf sparkConf = new SparkConf().setAppName("E-MapReduce Demo 5: Spark TableStore Demo (Java)");
        JavaSparkContext sc = null;
        try {
            sc = new JavaSparkContext(sparkConf);
            Configuration hadoopConf = new Configuration();
            TableStore.setCredential(
                    hadoopConf,
                    new Credential(accessKeyId, accessKeySecret, securityToken));
            Endpoint ep = new Endpoint(endpoint, instance);
            TableStore.setEndpoint(hadoopConf, ep);
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat.addCriteria(hadoopConf,
                    fetchCriteria(tableName, primaryKeyColumnName));
            JavaPairRDD<PrimaryKeyWritable, RowWritable> rdd = sc.newAPIHadoopRDD(
                    hadoopConf, com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat.class,
                    PrimaryKeyWritable.class, RowWritable.class);
            System.out.println(
                    new Formatter().format("TOTAL: %d", rdd.count()).toString());
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }
}
