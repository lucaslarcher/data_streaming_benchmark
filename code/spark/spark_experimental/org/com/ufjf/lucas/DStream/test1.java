package org.com.ufjf.lucas.DStream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class test1 {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));

        JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost",9999);

        JavaDStream<String> result = inputData.map(value -> value);

        JavaPairDStream<String,Long> pairDStream = result.mapToPair(message -> new Tuple2<String,Long>(message.split(" ")[0],1L));

        JavaPairDStream<String,Long> countDStream = pairDStream.reduceByKey((value1,value2) -> value1+value2);

        countDStream.foreachRDD(rdd -> rdd.foreach(value -> System.out.println(value._1+": "+value._2)));
        countDStream.print();

        sc.start();
        sc.awaitTermination();

    }
}
