package org.com.ufjf.lucas.StructedStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class KafkaStructuredStreamCountWords {
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


        //StreamingQuery query = cwdf.writeStream().format("console").outputMode(OutputMode.Complete()).start();

        //df.createOrReplaceTempView("viewing_figures");

        Dataset<Row> results = session.sql("select window, cast (value as string) as key,sum(1) from viewing_figures group by window(timestamp, '5 seconds')");

        //results.show();

        //List<String> listOne = df.as(Encoders.STRING()).collectAsList();
        //System.out.println(listOne);

        StreamingQuery query = results
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start();

        query.awaitTermination();
    }
}
