package com.aliyun.emr.example.storm.benchmark;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.NoRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCount extends BasicTopology {
    @Override
    protected void setBolt(TopologyBuilder builder) {
        int clusterCores = Integer.valueOf(configure.getProperty("cluster.cores.total"));
        int availableCores = clusterCores - Integer.valueOf(configure.getProperty("partition.number"));

        int hdfsParallelismFactor =  Integer.parseInt(configure.getProperty("hdfs.parallelism.factor"));
        int hdfsParallelism = availableCores * hdfsParallelismFactor / (hdfsParallelismFactor + 1);
        builder.setBolt("split-count", new SplitCount(), availableCores - hdfsParallelism).localOrShuffleGrouping("spout");

        String filenamePrefix = configure.getProperty("filename.prefix") + configure.getProperty("name") + "/";
        HdfsBolt bolt = new HdfsBolt()
            .withFsUrl(configure.getProperty("url"))
            .withFileNameFormat(new DefaultFileNameFormat().withPrefix(filenamePrefix))
            .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter(","))
            .withSyncPolicy(new CountSyncPolicy(1000))
            .withRotationPolicy(new NoRotationPolicy());
        builder.setBolt("hdfs-bolt", bolt, hdfsParallelism).localOrShuffleGrouping("split-count");
    }


    private class SplitCount extends BaseBasicBolt {
        private Map<String, Integer> counter = new HashMap<>();

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            Map<String, String> value = (Map<String, String>)input.getValue(0);
            for (Map.Entry<String, String>item : value.entrySet()) {
                String eventTime = item.getKey();
                String words = item.getValue();

                for (String word : words.split("\\s+")) {
                    Integer number = counter.get(word);
                    if (number == null) {
                        number = 0;
                    }
                    number++;
                    counter.put(word, number);
                }
                collector.emit(new Values(eventTime, System.currentTimeMillis()));
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("eventTime", "finishTime"));
        }
    }

    public static void main(String[] args) throws Exception {
        WordCount wordCount = new WordCount();
        if (args.length > 1) {
            if (!"--property".equals(args[1])) {
                System.out.println("unknow option: " + args[1]);
                System.out.println("usage storm jar examples-1.1-shaded.jar  com.aliyun.emr.example.storm.benchmark.WordCount benchmark.properties --property k1=v1,k2=v2");
                System.exit(1);
            }
            wordCount.init(args[0], args[2]);
        } else {
            wordCount.init(args[0]);
        }
        wordCount.run(true);
    }
}
