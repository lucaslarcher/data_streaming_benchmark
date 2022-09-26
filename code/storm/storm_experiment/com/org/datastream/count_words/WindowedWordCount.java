package com.org.datastream.count_words;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;


public class WindowedWordCount {
    public static Long count_values = 0L;

    public static void main(String[] args) throws Exception {
        String kafkaTopic = "stream-topic";
        String kafkaBootstrap = "192.168.0.181:9092";
        Integer num_workers = 4;
        String group_id = "stormgroupid";
        Integer default_parallelization = 4;

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
                .setProp(ConsumerConfig.GROUP_ID_CONFIG,"group_id01")
                .setProp(Config.TOPOLOGY_DEBUG, false)
                .setFirstPollOffsetStrategy(EARLIEST)
                //.setRetry(kafkaSpoutRetryService)
                .build();

        StreamBuilder builder = new StreamBuilder();

        builder.newStream(new KafkaSpout<String, String>(spoutConf), new ValueMapper<String>(4))

                .window(TumblingWindows.of(Duration.seconds(10)))

                .flatMap(s -> Arrays.asList(splitAndCount(s," ")))

                .mapToPair(w -> Pair.of(w, 1))

                .countByKey();


                //.print();

        Config config = new Config();
        String topoName = "word-count";
        if (args.length > 0) {
            topoName = args[0];
        }

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology(topoName, config, builder.build());
            Thread.sleep(10000000);
        }
        finally {
            cluster.shutdown();
        }
    }
    public static String[] splitAndCount(String text,String spliter){
        try {
            String[] words = text.split(spliter);
            for(String word:words) {
                count_values++;
                if (count_values % 1000000 == 0) {
                    SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                    Date date = new Date(System.currentTimeMillis());
                    System.out.println(formatter.format(date) + " " + String.valueOf(count_values));
                }
            }
            return words;
        }catch (Exception e){

        }
        return new String[0];
    }
}

