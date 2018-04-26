package com.aliyun.emr.example.storm.benchmark;

import com.aliyun.emr.example.storm.benchmark.util.Helper;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

abstract public class AbstractTopology implements Serializable{
    protected Properties configure;

    public void init(String configFilepath) throws Exception {
        init(configFilepath, "");
    }

    public void init(String configFilepath, String properties) throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(configFilepath));
        configure = new Properties();
        configure.load(in);

        if (! StringUtils.isBlank(properties)) {
            Map<String, String> customProperty = new HashMap<>();
            for (String item : properties.split(",")) {
                String[] kv = item.split("=");
                if (kv.length != 2) {
                    System.out.println("invalid property[" + item + "], pattern should be k1=v2,k2=v2...");
                    continue;
                }
                customProperty.put(kv[0], kv[1]);
            }
            configure.putAll(customProperty);
        }

        System.out.println("all configure: " + configure);
    }

    public void run(boolean cluster) throws Exception {
        String name = configure.getProperty("name");
        Config conf = new Config();

        if (!cluster) {
            new LocalCluster().submitTopology("local-" + name, conf, createTopology());
            return;
        }

        int slots = Integer.valueOf(configure.getProperty("worker.slot.number"));
        int clusterNodes = Integer.valueOf(configure.getProperty("cluster.worker.node.number"));
        int workerNumber = slots * clusterNodes;
        int clusterNodeMemoryMb = Integer.valueOf(configure.getProperty("cluster.memory.per.node.mb"));
        int workerMem = clusterNodeMemoryMb / slots;
        conf.setNumWorkers(workerNumber);
        if (!Boolean.valueOf(configure.getProperty("ack.open"))) {
            conf.setNumAckers(0);
        }

        conf.put("worker.heap.memory.mb", workerMem);
        conf.put("topology.backpressure.enable", Boolean.valueOf(configure.getProperty("backpressure.enable")));
        StormSubmitter.submitTopologyWithProgressBar(name, conf, createTopology());
        Helper.setupShutdownHook(name); // handle Ctrl-C

        System.out.println("**********metrics will begin in two minute, please start to send source data to warn up**********");
        for (int i = 0; i< 2; i++) {
            Thread.sleep(1000 * 60);
            System.out.println("...");
        }
        System.out.println("********** start metrics **********");
        Helper.collectMetrics(name, 60);
    }

    abstract StormTopology createTopology();
}
