package com.org.datastream.count_words;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNomalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-normalizer",new Fields("word"));

        Config conf = new Config();
        conf.put("fileToRead","/home/lucas/Documents/data_stream/storm/storm_experiment/com/org/datastream/count_words/test.txt");
        conf.put("dirToWrite","/home/lucas/Documents/data_stream/storm/storm_experiment/com/org/datastream/count_words/output/");

        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("WordCounter-Topology", conf, builder.createTopology());
            Thread.sleep(30000);
        }
        finally {
            cluster.shutdown();
        }

    }
}
