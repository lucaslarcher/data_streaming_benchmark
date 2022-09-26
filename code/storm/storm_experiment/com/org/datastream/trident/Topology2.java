package com.org.datastream.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class Topology2 {
    public static void main(String[] args) throws Exception {
        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("simple",drpc)
                .map(new lowercase())
                .flatMap(new split())
                .filter(new filterShort());

        Config config = new Config();
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident-topology",config,topology.build());

        for (String word : new String[]{"First Page","Second Line","Third Word in The Book"}){
            System.out.println("Resulto for "+word+" : "+drpc.execute("simple",word));
        }
    }
}
