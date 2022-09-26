package com.org.datastream.count_words_kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

public class WindowKafkaTest {
    public static void main(String[] args) throws Exception {

        String kafkaTopic = "stream-topic";
        String kafkaBootstrap = "192.168.0.181:9092";
        String group_id = "stormgroupid";
        Number default_parallelization = 16;

        if(args.length > 0){
            kafkaBootstrap = args[0];
        }
        if(args.length > 1){
            group_id = args[1];
        }
        if(args.length > 2){
            default_parallelization = Integer.parseInt(args[2]);
        }
        if(args.length > 3){
            kafkaTopic = args[3];
        }

        System.out.println("ARGS => bootstrap_server group_id default_parallelism topic");
        System.out.println("BootsTrap Server: "+kafkaBootstrap);
        System.out.println("Gruop Id: "+group_id);
        System.out.println("Parallelism: "+default_parallelization);
        System.out.println("Topic: "+kafkaTopic);

        KafkaSpoutConfig spoutConf =  KafkaSpoutConfig.builder(kafkaBootstrap, kafkaTopic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG,"groupidabcde")
                .setProp(Config.TOPOLOGY_DEBUG, false)
                .setFirstPollOffsetStrategy(EARLIEST)
                //.setRetry(kafkaSpoutRetryService)
                .build();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout<String, String>(spoutConf));
        builder.setBolt("word-normalizer", new WordNomalizer(),default_parallelization).shuffleGrouping("kafka_spout");
        builder.setBolt("word-counter", new WBolt().withTumblingWindow(BaseWindowedBolt.Duration.seconds(10)),default_parallelization).fieldsGrouping("word-normalizer",new Fields("word"));

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
