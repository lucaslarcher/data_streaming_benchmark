package com.org.datastream.count_words_streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;


public class KafkaTopology {

    public static Long count_values;

    public static void main(String[] args) throws Exception {

        String kafkaTopic = "stream-topic";
        String kafkaBootstrap = "192.168.0.181:9092";


        count_values = 0L;


        KafkaSpoutConfig spoutConf = KafkaSpoutConfig.builder(kafkaBootstrap, kafkaTopic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG,"groupidabcd")//carefull, can bug a offset name with "_" for example
                .setProp(Config.TOPOLOGY_DEBUG, false)
                //.setProp(Config.SUPERVISOR_WORKER_HEARTBEATS_MAX_TIMEOUT_SECS, 600)
                //.setProp("socket.receive.buffer.bytes",102400)
                //.setProp("socket.send.buffer.bytes",102400)
                //.setProp("queued.max.requests",500)
                //.setProp("message.max.bytes",1000000)
                //.setProp("socket.request.max.bytes",104857600)
                //.setProp("zookeeper.connection.timeout.ms",50000)
                .setFirstPollOffsetStrategy(EARLIEST)
                //.setRetry(kafkaSpoutRetryService)
                .build();





        /*
        //BrokerHosts hosts = new ZkHosts(kafkaBootstrap);

        Broker brokerForPartition0 = new Broker(kafkaBootstrap.split(":")[0], Integer.parseInt(kafkaBootstrap.split(":")[1]));//localhost:9092
        GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation(kafkaTopic);
        partitionInfo.addPartition(0, brokerForPartition0);//mapping form partition 0 to brokerForPartition0
        StaticHosts hosts = new StaticHosts(partitionInfo);

        SpoutConfig spoutConfig = new SpoutConfig(hosts,kafkaTopic,"/" + kafkaTopic, "groupid");
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        */

        StreamBuilder builder = new StreamBuilder();

        builder
                // A stream of random sentences with two partitions
                .newStream(new KafkaSpout<String,String>(spoutConf), new ValueMapper<String>(4))
                // a two seconds tumbling window
                .window(TumblingWindows.of(BaseWindowedBolt.Duration.seconds(5)))
                .flatMap(s -> Arrays.asList(splitAndCount(s," ")))
                // create a stream of (word, 1) pairs
                .mapToPair(w -> Pair.of(w, 1))
                .countByKey()
                .print();

        Config config = new Config();
        config.setDebug(false);
        String topoName = "Storm-Count-Word-Window-Kafka_Topology";
        config.setNumWorkers(8);

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology(topoName, config, builder.build());
            Thread.sleep(10000000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            cluster.shutdown();
        }

    }

    public static String[] splitAndCount(String text,String spliter){
        try {
            String[] words = text.split(spliter);
            for(String word:words) {
                count_values++;
                if (count_values % 100000 == 0) {
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
