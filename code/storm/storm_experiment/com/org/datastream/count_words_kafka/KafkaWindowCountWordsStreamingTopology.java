package com.org.datastream.count_words_kafka;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

public class KafkaWindowCountWordsStreamingTopology {

   public static void main(String[] args) throws Exception {

        String kafkaTopic = "stream-topic";
        String kafkaBootstrap = "192.168.0.181:9092";
        Integer num_workers = 4;
        String group_id = "stormgroupid";
        Integer default_parallelization = 4;

        for(String arg:args){
            System.out.println(arg);
        }

        if(args.length > 0){
            kafkaBootstrap = args[0];
        }
        if(args.length > 1){
            group_id = args[1];
        }
        if(args.length > 2){
            num_workers = Integer.parseInt(args[2]);
        }
        if(args.length > 3){
            default_parallelization = Integer.parseInt(args[3]);
        }
        if(args.length > 4){
            kafkaTopic = args[4];
        }

        //kafkaBootstrap = "localhost:9092";

        System.out.println("ARGS => bootstrap_server group_id num_workers default_paralelism topic");
        System.out.println("BootsTrap Server: "+kafkaBootstrap);
        System.out.println("Gruop Id: "+group_id);
        System.out.println("Cores: "+num_workers);
        System.out.println("Parallelism: "+default_parallelization);
        System.out.println("Topic: "+kafkaTopic);

        KafkaSpoutConfig spoutConf =  KafkaSpoutConfig.builder(kafkaBootstrap, kafkaTopic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG,group_id)
                .setProp(Config.TOPOLOGY_DEBUG, false)
                .setFirstPollOffsetStrategy(EARLIEST)
                .build();


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout<String, String>(spoutConf));
        //builder.setBolt("window", new WindowedBolt().withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS))).shuffleGrouping("kafka_spout");
        builder.setBolt("window", new WBolt().withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS)),default_parallelization).shuffleGrouping("kafka_spout");
        builder.setBolt("word-normalizer", new WindowWordNomalizer(),default_parallelization).fieldsGrouping("window",new Fields("line"));
        builder.setBolt("word-counter", new WordCounter(),default_parallelization).fieldsGrouping("word-normalizer",new Fields("word"));

        Config conf = new Config();
        conf.setNumWorkers(num_workers);
        conf.setDebug(false);


        LocalCluster cluster = new LocalCluster();

        //Kafka server up?
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        final int ADMIN_CLIENT_TIMEOUT_MS = 2000;
        boolean kafka_up = false;
        while(!kafka_up) {
            try (AdminClient client = AdminClient.create(properties)) {
                client.listTopics(new ListTopicsOptions().timeoutMs(ADMIN_CLIENT_TIMEOUT_MS)).listings().get();
                kafka_up = true;
            } catch (ExecutionException ex) {
                System.out.println("Kafka is not available, timed out after "+ADMIN_CLIENT_TIMEOUT_MS+" ms ");
                kafka_up = false;
                Thread.sleep(ADMIN_CLIENT_TIMEOUT_MS);
            }
        }

        try {
            cluster.submitTopology("WordCounter-Topology", conf, builder.createTopology());
            //Thread.sleep(300000);
        }
        finally {
            //cluster.shutdown();
        }
    }
}

