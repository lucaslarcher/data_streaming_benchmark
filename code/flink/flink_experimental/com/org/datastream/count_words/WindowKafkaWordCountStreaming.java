package com.org.datastream.count_words;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class WindowKafkaWordCountStreaming {

    public static Long count_values = 0L;

    public static void main(String[] args) throws Exception
    {
        String bootstrap_server = "192.168.0.181:9092";
        String grupid = "flinkgroupid";
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
            parallelism = args[2];
        }
        if(args.length > 3){
            topic = args[3];
        }

        System.out.println("ARGS => bootstrap_server group_id parallelism topic");
        System.out.println("BootsTrap Server: "+bootstrap_server);
        System.out.println("Group Id: "+grupid);
        System.out.println("Parallelism: "+parallelism);
        System.out.println("Topic: "+topic);

        final StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();


        // Checking input parameters
        //final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        //env.getConfig().setGlobalJobParameters(params);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap_server);
        properties.setProperty("group.id", grupid);
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
                        for (String word : words) {
                            out.collect(new Tuple2<String, Integer>(word, 1));
                            count_values++;
                            if (count_values % 1000000 == 0) {
                                SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                                Date date = new Date(System.currentTimeMillis());
                                System.out.println(formatter.format(date) + " " + String.valueOf(count_values));
                            }
                        }
                    }	})

                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1); // group by the tuple field "0" and sum up tuple field "1"

        //counts.print();



        // execute program
        env.setParallelism(Integer.parseInt(parallelism));

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
