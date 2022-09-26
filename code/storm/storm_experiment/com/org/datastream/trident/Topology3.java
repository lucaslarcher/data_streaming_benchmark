package com.org.datastream.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.streams.operations.aggregators.Count;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.tuple.Fields;

public class Topology3 {
    public static void main(String[] args) throws Exception {
        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("simple",drpc)
                .map(new lowercase())
                .flatMap(new split())
                .aggregate(new CountAsAggregator(), new Fields("count"));

        Config config = new Config();
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-topology",config,topology.build());

        for (String word : new String[]{"First Page","Second Line","Third Word in The Book"}){
            System.out.println("Resulto for "+word+" : "+drpc.execute("simple",word));
        }
    }
}
