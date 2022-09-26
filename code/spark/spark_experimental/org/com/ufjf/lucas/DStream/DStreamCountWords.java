package org.com.ufjf.lucas.DStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class DStreamCountWords {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost",9999);

        JavaDStream<String> result = inputData.map(value -> value);

        JavaDStream<String> singleWords = result.flatMap(value -> Arrays.asList(value.toLowerCase().split(" ")).iterator());

        JavaPairDStream<String,Long> pairDStream = singleWords.mapToPair(value -> new Tuple2<String,Long>(value,1L));

        //JavaPairDStream<String,Long> countDStream = pairDStream.reduceByKey((value1,value2) -> value1+value2);
        JavaPairDStream<String,Long> countDStream = pairDStream.reduceByKeyAndWindow((value1,value2) -> value1+value2, Durations.seconds(5));

        //countDStream.foreachRDD(rdd -> rdd.foreach(value -> System.out.println(value._1+": "+value._2)));
        countDStream.print();

        sc.start();
        sc.awaitTermination();

    }
}
