package com.aliyun.emr.example.storm.benchmark;

import kafka.api.OffsetRequest;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.rotation.NoRotationPolicy;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.StringKeyValueScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class TridentWordCount extends AbstractTopology {

    @Override
    StormTopology createTopology() {
        int partition = Integer.valueOf(configure.getProperty("partition.number"));

        TridentTopology topology = new TridentTopology();
        TransactionalTridentKafkaSpout spout = createSpout();

        topology.newStream("kafka-spout", spout).name("kafka").parallelismHint(partition)
            .each(spout.getOutputFields(), new WordCount(), new Fields("eventTime", "finishTime")).name("word-count")
            .partitionPersist(createHdfsState("eventTime", "finishTime"), new Fields("eventTime", "finishTime"), new HdfsUpdater(), new Fields("eventTime", "finishTime"));
        return topology.build();
    }

    private TransactionalTridentKafkaSpout createSpout() {
        String consumerGroup = configure.getProperty("consumer.group");
        ZkHosts zkHost = new ZkHosts(configure.getProperty("zookeeper.address") + ":2181" + configure.getProperty("zookeeper.root"));
        TridentKafkaConfig config = new TridentKafkaConfig(zkHost, configure.getProperty("topic"), consumerGroup);
        config.socketTimeoutMs = 60 * 1000;
        config.ignoreZkOffsets=true;
        config.startOffsetTime= OffsetRequest.LatestTime();
        config.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
        config.startOffsetTime = OffsetRequest.LatestTime();
        return new TransactionalTridentKafkaSpout(config);
    }

    private StateFactory createHdfsState(String... fileds) {
        String filenamePrefix = configure.getProperty("filename.prefix") + configure.getProperty("name") + "/";

        HdfsState.Options options =  new HdfsState.HdfsFileOptions()
            .withFsUrl(configure.getProperty("url"))
            .withFileNameFormat(new DefaultFileNameFormat().withPath(filenamePrefix))
            .withRecordFormat(new DelimitedRecordFormat().withFields(new Fields(fileds)))
            .withRotationPolicy(new NoRotationPolicy());
        return new HdfsStateFactory().withOptions(options);
    }

    private class WordCount extends BaseFunction {
        private HashMap<String, Integer> count = new HashMap<>();
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            // for test
            Map<String, String> kv = (Map<String, String>)tuple.get(0);
            for (Map.Entry<String, String> item: kv.entrySet()) {
                String eventTime = item.getKey();
                String words = item.getValue();
                for  (String word: words.split("\\s+")) {
                    Integer number = count.get(word);
                    if (number == null) {
                        number = 0;
                    }
                    number++;
                    count.put(word, number);

                }
                collector.emit(new Values(eventTime, System.currentTimeMillis()));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        TridentWordCount wordCount = new TridentWordCount();
        if (args.length > 1) {
            if (!"--property".equals(args[1])) {
                System.out.println("unknow option: " + args[1]);
                System.out.println("usage storm jar examples-1.1-shaded.jar  com.aliyun.emr.example.storm.benchmark.TridentWordCount benchmark.properties --property k1=v1,k2=v2");
                System.exit(1);
            }
            wordCount.init(args[0], args[2]);
        } else {
            wordCount.init(args[0]);
        }
        wordCount.run(true);
    }
}
