package org.com.ufjf.lucas.DStream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaDStreamCountWords {
    public static Long count_values;

    public static void main(String[] args) throws InterruptedException {
        String bootstrap_server = "192.168.0.181:9092";
        String grupid = "sparkgroupid";
        String cores = "4";
        String parallelism = "8";
        String topic = "stream-topic";
        count_values = 0L;

        if(args.length > 0){
            bootstrap_server = args[0];
        }
        if(args.length > 1){
            grupid = args[1];
        }
        if(args.length > 2){
            cores = args[2];
        }
        if(args.length > 3){
            parallelism = args[3];
        }
        if(args.length > 4){
            topic = args[4];
        }

        bootstrap_server = "localhost:9092";

        System.out.println("ARGS => bootstrap_server group_id cores paralelism topic");
        System.out.println("BootsTrap Server: "+bootstrap_server);
        System.out.println("Gruop Id: "+grupid);
        System.out.println("Cores: "+cores);
        System.out.println("Parallelism: "+parallelism);
        System.out.println("Topic: "+topic);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        conf.set("spark.executor.cores", cores);
        conf.set("spark.default.parallelism",parallelism);
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));

        Map<String, Object> kafkaParams = new HashMap<String,Object>();
        kafkaParams.put("bootstrap.servers", bootstrap_server);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", grupid);
        kafkaParams.put("auto.offset.reset", "earliest");

        Collection<String> topics = Arrays.asList(topic);

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc,LocationStrategies.PreferConsistent(),ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaDStream<String> result = stream.map(value -> value.value());

        JavaDStream<String> singleWords = result.flatMap(value -> Arrays.asList(splitAndCount(value.toLowerCase()," ")).iterator());

        JavaPairDStream<String,Long> pairDStream = singleWords.mapToPair(value -> new Tuple2<String,Long>(value,1L));

        //JavaPairDStream<String,Long> countDStream = pairDStream.reduceByKey((value1,value2) -> value1+value2);
        JavaPairDStream<String,Long> countDStream = pairDStream.reduceByKey((value1,value2) -> value1+value2);

        countDStream.foreachRDD(rdd->{});
        //countDStream.foreachRDD(rdd -> rdd.foreach(value -> System.out.println(value._1+": "+value._2)));
        //countDStream.print();

        //Kafka server up?
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        final int ADMIN_CLIENT_TIMEOUT_MS = 1000;
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

        sc.start();
        sc.awaitTermination();

    }

    public static String[] splitAndCount(String text,String spliter){
        count_values++;
        if (count_values%1000 == 0){
            SimpleDateFormat formatter= new SimpleDateFormat("HH:mm:ss");
            Date date = new Date(System.currentTimeMillis());
            System.out.println(formatter.format(date)+" "+String.valueOf(count_values));
        }
        return text.split(spliter);
    }
}
