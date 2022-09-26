package com.org.datastream.count_words_kafka;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WordNomalizer extends BaseBasicBolt {

    static Long count_values = 0L;

    @Override
    public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {
        try {
            String sentence = input.getString(4);
            String[] words = sentence.split(" ");
            for (String word : words) {
                word = word.trim();
                if (!word.isEmpty()) {
                    word = word.toLowerCase();
                    basicOutputCollector.emit(new Values(word));
                }
                count_values++;

                if (count_values % 1000000 == 0) {
                    SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                    Date date = new Date(System.currentTimeMillis());
                    System.out.println(formatter.format(date) + " " + String.valueOf(count_values));
                }
            }
        }catch (Exception e){

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
