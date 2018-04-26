package com.aliyun.emr.example.storm.benchmark;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.NoRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class KafkaHdfs extends BasicTopology{

    protected void setBolt(TopologyBuilder builder) {
        int clusterCores = Integer.valueOf(configure.getProperty("cluster.cores.total"));
        int availableCores = clusterCores - Integer.valueOf(configure.getProperty("partition.number"));

        builder.setBolt("hdfs-bolt", getHdfsBolt(), availableCores).localOrShuffleGrouping("spout");
    }

    private IRichBolt getHdfsBolt() {

        String filenamePrefix = configure.getProperty("filename.prefix") + configure.getProperty("name") + "/";
        HdfsBolt bolt = new HdfsBolt()
            .withFsUrl(configure.getProperty("url"))
            .withFileNameFormat(new DefaultFileNameFormat().withPrefix(filenamePrefix))
            .withRecordFormat(new RecordFormat() {
                @Override
                public byte[] format(Tuple tuple) {
                    String eventTime = ((Map<String, String>)tuple.getValue(0)).keySet().iterator().next();
                    String output = eventTime + "," + System.currentTimeMillis() + System.lineSeparator();
                    return output.getBytes();
                }
            })
            .withSyncPolicy(new CountSyncPolicy(1000))
            .withRotationPolicy(new NoRotationPolicy());
        return bolt;
    }

    public static void main(String[] args) throws Exception {
        KafkaHdfs topology = new KafkaHdfs();
        if (args.length > 1) {
            if (!"--property".equals(args[1])) {
                System.out.println("unknow option: " + args[1]);
                System.out.println("usage storm jar examples-1.1-shaded.jar  com.aliyun.emr.example.storm.benchmark.KafkaHdfs benchmark.properties --property k1=v1,k2=v2");
                System.exit(1);
            }
            topology.init(args[0], args[2]);
        } else {
            topology.init(args[0]);
        }

        topology.run(true);
    }
}
