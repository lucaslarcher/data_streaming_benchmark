package com.org.datastream.count_words;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordNomalizer extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for (String word:words){
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                basicOutputCollector.emit(new Values(word));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
