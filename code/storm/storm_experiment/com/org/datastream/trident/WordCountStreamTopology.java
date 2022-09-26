package com.org.datastream.trident;

import com.org.datastream.count_words.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

public class WordCountStreamTopology {
    public static void main(String[] args) throws Exception {
        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        Stream wordCounts = topology.newStream("spout1", new WordReader())
                .each(new Fields("line"), new Split(), new Fields("word"))
                .parallelismHint(16)
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .newValuesStream()
                .parallelismHint(16);

        Config config = new Config();
        config.put("fileToRead","/home/lucas/Documents/data_stream/storm/storm_experiment/com/org/datastream/count_words/test.txt");
        config.put("dirToWrite","/home/lucas/Documents/data_stream/storm/storm_experiment/com/org/datastream/count_words/output/");

        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-topology",config,topology.build());

        for (String word : new String[]{"First Page","Second Line","Third Word in The Book"}){
            System.out.println("Resulto for "+word+" : "+drpc.execute("simple",word));
        }
    }
}
