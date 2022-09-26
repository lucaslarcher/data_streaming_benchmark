package com.org.datastream.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class Topology {
    public static void main(String[] args) throws Exception {
        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("simple",drpc)
                .each(new Fields("args"),
                        new simpleFunction(),
                        new Fields("processed_word"));

        Config config = new Config();
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-topology",config,topology.build());

        for (String word : new String[]{"word 1","word 2","word 3"}){
            System.out.println("Resulto for "+word+" : "+drpc.execute("simple",word));
        }
    }
}
