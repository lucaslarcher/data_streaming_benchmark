package org.com.ufjf.lucas.StructedStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.kafka010.*;


import java.util.concurrent.TimeoutException;

public class test1 {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("structuredStreaming")
                .getOrCreate();

        Dataset<Row> df = session
                .readStream()
                //.format("kafka")
                .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe","stream-topic")
                //.option("startingOffsets", "earliest")
                .load();

        df.createOrReplaceTempView("viewing_figures");

        Dataset<Row> results = session.sql("select window,cast (value as string) as key from viewing_figures");

        StreamingQuery query = results
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start();

        query.awaitTermination();
    }
}
