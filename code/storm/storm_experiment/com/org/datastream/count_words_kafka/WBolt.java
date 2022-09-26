package com.org.datastream.count_words_kafka;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;


import java.util.HashMap;
import java.util.Map;

public class WBolt extends BaseWindowedBolt {
    Integer id;
    String name;
    Map<String,Integer> counters;
    String fileName;

    public void prepare(Map stormConf, TopologyContext context){
        this.counters = new HashMap<String,Integer>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        try {
            for(Tuple tuple: tupleWindow.get()) {
                String str = tuple.getString(0);

                if (!counters.containsKey(str)) {
                    counters.put(str, 1);
                } else {
                    Integer c = counters.get(str) + 1;
                    counters.put(str, c);
                }
                //for (String keys : counters.keySet()) {
                //  System.out.println(keys + " : " + counters.get(keys));
                //}
            }
        }catch (Exception e){

        }

    }
}
