package org.com.ufjf.lucas;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class test {

    public static void main(String[] args) throws InterruptedException {

        //String bootstrap_server = "10.5.18.150:11020";
        String bootstrap_server = "localhost:9092";
        if(args.length > 0){
            bootstrap_server = args[0];
            System.out.println("BootsTrap Server: "+bootstrap_server);
        }
    // Configure Spark to connect to Kafka running on local machine
    Map<String, Object> kafkaParams = new HashMap<String,Object>();
        kafkaParams.put("bootstrap.servers", bootstrap_server);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer .class);
        kafkaParams.put("group.id", "mygroup1");
        kafkaParams.put("auto.offset.reset", "earliest");

    //Configure Spark to listen messages in topic test
    Collection<String> topics = Arrays.asList("test");

    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkKafka10WordCount");

    //Read messages in batch of 30 seconds
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));

    // Start reading messages from Kafka and get DStream
    final JavaInputDStream<ConsumerRecord<String, String>> stream =
            KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams));

    // Read value of each message from Kafka and return it
    JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String,String>, String>() {
        @Override
        public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
            return kafkaRecord.value();
        }
    });

    // Break every message into words and return list of words
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
        @Override
        public Iterator<String> call(String line) throws Exception {
            return Arrays.asList(line.split(" ")).iterator();
        }
    });

    // Take every word and return Tuple with (word,1)
    JavaPairDStream<String,Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String word) throws Exception {
            return new Tuple2<>(word,1);
        }
    });

    // Count occurance of each word
    JavaPairDStream<String,Integer> wordCount = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer first, Integer second) throws Exception {
            return first+second;
        }
    });

    //Print the word count
        wordCount.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
