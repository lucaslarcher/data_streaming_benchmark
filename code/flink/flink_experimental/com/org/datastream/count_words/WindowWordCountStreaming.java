package com.org.datastream.count_words;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class WindowWordCountStreaming {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> data = env.socketTextStream("localhost", 9999);


        DataStream<Tuple2<String, Integer>> counts =  data
                .map(new WordCountStreaming.Tokenizer())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                .sum(1);           // group by the tuple field "0" and sum up tuple field "1"

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
