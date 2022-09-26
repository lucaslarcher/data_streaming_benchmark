package com.org.datastream.count_words_kafka;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

public class WindowBolt implements IWindowedBolt {
    public OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for(Tuple tuple : tupleWindow.get()){
            collector.emit(new Values(tuple.getString(4)));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public TimestampExtractor getTimestampExtractor() {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        try {
            outputFieldsDeclarer.declare(new Fields("line"));
        } catch (Exception e) {
            //System.out.println("declareOutputFields");
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
