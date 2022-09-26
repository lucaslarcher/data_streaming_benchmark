package com.org.datastream.count_words_kafka;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class WordCounter extends BaseBasicBolt {

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
    public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {
        try {
            String str = input.getString(0);

            if (!counters.containsKey(str)) {
                counters.put(str, 1);
            } else {
                Integer c = counters.get(str) + 1;
                counters.put(str, c);
            }
            //for (String keys : counters.keySet()) {
              //  System.out.println(keys + " : " + counters.get(keys));
            //}
        }catch (Exception e){

        }
    }

    public void cleanup(){
        try {
            for (String keys : counters.keySet()) {
                System.out.println(keys + " : " + counters.get(keys));
            }
        }catch (Exception e){

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
