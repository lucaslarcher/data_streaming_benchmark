package com.org.datastream.count_words_kafka;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.tuple.Fields;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;


public class KafkaTest {
    private static final Logger LOG = Logger.getLogger(KafkaTest.class);

    public static void main(String[] args) throws Exception {

        String kafkaTopic = "stream-topic";
        String kafkaBootstrap = "192.168.0.181:9092";

        KafkaSpoutConfig spoutConf =  KafkaSpoutConfig.builder(kafkaBootstrap, kafkaTopic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG,"groupida")
                .setProp(Config.TOPOLOGY_DEBUG, false)
                .setFirstPollOffsetStrategy(EARLIEST)
                //.setRetry(kafkaSpoutRetryService)
                .build();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout<String, String>(spoutConf));
        builder.setBolt("word-normalizer", new WordNomalizer(),8).shuffleGrouping("kafka_spout");
        builder.setBolt("word-counter", new WordCounter(),8).fieldsGrouping("word-normalizer",new Fields("word"));

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("WordCounter-Topology", conf, builder.createTopology());
            Thread.sleep(1000000);
        }
        finally {
            cluster.shutdown();
        }
    }
}


