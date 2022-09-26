package com.org.datastream.count_words_streaming;

import com.org.datastream.count_words.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.util.Arrays;

public class Topology {

    public static void main(String[] args) throws Exception {

        StreamBuilder builder = new StreamBuilder();

        builder
                // A stream of random sentences with two partitions
                .newStream(new RandomSentenceSpout(), new ValueMapper<String>(0), 2)
                // a two seconds tumbling window
                .window(TumblingWindows.of(BaseWindowedBolt.Duration.seconds(2)))
                // split the sentences to words
                .flatMap(s -> Arrays.asList(s.split(" ")))
                // create a stream of (word, 1) pairs
                .mapToPair(w -> Pair.of(w, 1))
                // compute the word counts in the last two second window
                .countByKey()
                // print the results to stdout
                .print();

        Config config = new Config();
        String topoName = "test";
        if (args.length > 0) {
            topoName = args[0];
        }
        config.setNumWorkers(1);

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology(topoName, config, builder.build());
            Thread.sleep(300000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            cluster.shutdown();
        }

    }
}
