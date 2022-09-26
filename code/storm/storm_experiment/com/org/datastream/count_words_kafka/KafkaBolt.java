package com.org.datastream.count_words_kafka;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class KafkaBolt extends BaseBasicBolt {

    private static final Logger LOG = Logger.getLogger(KafkaBolt.class);

    @Override
    public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {
        String sentence = input.getString(4);
        System.out.println(sentence);
        String[] words = sentence.split(" ");
        for (String word:words){
            word = word.trim();
            if(!word.isEmpty()){
                basicOutputCollector.emit(new Values(word));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
