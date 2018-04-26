package com.aliyun.emr.example.storm.benchmark;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.NoRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;

import java.util.HashMap;
import java.util.Map;

public class WindowedWordCount extends BasicTopology {
    @Override
    protected void setBolt(TopologyBuilder builder) {
        int windowLength = Integer.valueOf(configure.getProperty("window.length"));
        int clusterCores = Integer.valueOf(configure.getProperty("cluster.cores.total"));
        int availableCores = clusterCores - Integer.valueOf(configure.getProperty("partition.number"));
        int parallelism =  availableCores / 2;

        int slidingInterval = Integer.valueOf(configure.getProperty("slide.interval"));

        builder.setBolt("count", new SplitCount().withWindow(new Count(windowLength), new Count(slidingInterval)), parallelism).localOrShuffleGrouping("spout");

        String filenamePrefix = configure.getProperty("filename.prefix") + configure.getProperty("name") + "/";
        HdfsBolt bolt = new HdfsBolt()
            .withFsUrl(configure.getProperty("url"))
            .withFileNameFormat(new DefaultFileNameFormat().withPrefix(filenamePrefix))
            .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter(","))
            .withSyncPolicy(new CountSyncPolicy(1000))
            .withRotationPolicy(new NoRotationPolicy());
        builder.setBolt("hdfs-bolt", bolt, parallelism).localOrShuffleGrouping("count");
    }

    private class SplitCount extends BaseWindowedBolt {
        private OutputCollector collector;
        private Map<String, Integer> counter = new HashMap<>();

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            super.prepare(stormConf, context, collector);
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            for ( Tuple tuple : inputWindow.get()) {
                Map<String, String> value = (Map)tuple.getValue(0);
                for (Map.Entry<String, String> item : value.entrySet()) {
                    String eventTime = item.getKey();
                    String words = item.getValue();
                    for (String word: words.split("\\s+")) {
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

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("eventTime", "finishTime"));
        }
    }

    public static void main(String[] args) throws Exception {
        WindowedWordCount wordCount = new WindowedWordCount();
        if (args.length > 1) {
            if (!"--property".equals(args[1])) {
                System.out.println("unknow option: " + args[1]);
                System.out.println("usage storm jar examples-1.1-shaded.jar  com.aliyun.emr.example.storm.benchmark.WindowedWordCount benchmark.properties --property k1=v1,k2=v2");
                System.exit(1);
            }
            wordCount.init(args[0], args[2]);
        } else {
            wordCount.init(args[0]);
        }
        wordCount.run(true);
    }
}
