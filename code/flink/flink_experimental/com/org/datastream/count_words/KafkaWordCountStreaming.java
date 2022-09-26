package com.org.datastream.count_words;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaWordCountStreaming {

    public static void main(String[] args) throws Exception
    {

        final StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "stream-topic";
        String group_id = "group_id";

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.181:9092");
        properties.setProperty("group.id", group_id);
        // Checking input parameters
        //final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        //env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(),  properties).setStartFromEarliest());

        DataStream<Tuple2<String, Integer>> counts =  text
        .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>()
                {
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                    {
                        String[] words = value.split(" ");
                        for (String word : words)
                            out.collect(new Tuple2<String, Integer>(word, 1));
                    }	})

                .keyBy(0)
                .sum(1); // group by the tuple field "0" and sum up tuple field "1"

        counts.print();

        // execute program
        env.execute("Streaming WordCount");
    }

    public static final class Tokenizer  implements MapFunction<String, Tuple2<String, Integer>>
    {
        public Tuple2<String, Integer> map(String value)
        {
            return new Tuple2(value, Integer.valueOf(1));
        }
    }
}
