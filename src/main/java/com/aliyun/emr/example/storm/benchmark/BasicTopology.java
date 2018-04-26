package com.aliyun.emr.example.storm.benchmark;

import com.google.common.collect.ImmutableMap;
import kafka.api.OffsetRequest;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.Properties;

public class BasicTopology extends AbstractTopology {

    @Override
    StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        setSpout(builder);
        setBolt(builder);
        return builder.createTopology();
    }

    private void setSpout(TopologyBuilder builder) {
        String consumerGroup = configure.getProperty("consumer.group");
        SpoutConfig conf = new SpoutConfig(new ZkHosts(
            configure.getProperty("zookeeper.address") + ":2181" + configure.getProperty("zookeeper.root")),
            configure.getProperty("topic"), configure.getProperty("zookeeper.root"), consumerGroup);
        conf.zkPort = 2181;
        conf.zkServers= Arrays.asList(configure.getProperty("zookeeper.address"));
        conf.socketTimeoutMs = 60 * 1000;
        conf.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
        conf.startOffsetTime= OffsetRequest.LatestTime();
        conf.ignoreZkOffsets = true;
        KafkaSpout spout = new KafkaSpout(conf);

        int kafkaPartition = Integer.valueOf(configure.getProperty("partition.number"));
        builder.setSpout("spout", spout, kafkaPartition);
    }

    protected void setBolt(TopologyBuilder builder) {
        int clusterCores = Integer.valueOf(configure.getProperty("cluster.cores.total"));
        int availableCores = clusterCores - Integer.valueOf(configure.getProperty("partition.number"));


        //inter bolt
        //builder.setBolt("inter-bolt", getInterBolt(), availableCores).localOrShuffleGrouping("spout");

        //kafka storm-bolt
        builder.setBolt("kafka-bolt", getKafkaBolt(), availableCores).localOrShuffleGrouping("spout");
    }

    private IBasicBolt getInterBolt() {
        return  new BaseBasicBolt() {
            @Override
            public void execute(Tuple input, BasicOutputCollector collector) {
                collector.emit(new Values(input));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("inter-bolt"));
            }
        };
    }

    private IRichBolt getKafkaBolt() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", configure.getProperty("result.broker.list"));
        properties.put("acks", "0");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // consume too much memory
        //properties.put("batch.size", "10485760");
        //properties.put("max.request", "10485760");
        //properties.put("send.buffer.bytes", "1000000");
        KafkaBolt bolt = new KafkaBolt<String, String>()
            .withProducerProperties(properties)
            .withTopicSelector(new DefaultTopicSelector(configure.getProperty("result.topic")))
            .withTupleToKafkaMapper(new TupleToKafkaMapper<String, String>() {
                @Override
                public String getKeyFromTuple(Tuple tuple) {
                    return null;
                }

                @Override
                public String getMessageFromTuple(Tuple tuple) {

                    ImmutableMap<String, String> kv = (ImmutableMap<String, String>)tuple.getValue(0);
                    return kv.keySet().iterator().next() + "," + System.currentTimeMillis();

                }
            });
        bolt.setFireAndForget(true);
        bolt.setAsync(true);
        return bolt;
    }

    public static void main(String[] args) throws Exception {
        BasicTopology basicTopology = new BasicTopology();
        if (args.length > 1) {
            if (!"--property".equals(args[1])) {
                System.out.println("unknow option: " + args[1]);
                System.out.println("usage storm jar examples-1.1-shaded.jar  com.aliyun.emr.example.storm.benchmark.BasicTopology benchmark.properties --property k1=v1,k2=v2");
                System.exit(1);
            }
            basicTopology.init(args[0], args[2]);
        } else {
            basicTopology.init(args[0]);
        }

        basicTopology.run(true);
    }
}
