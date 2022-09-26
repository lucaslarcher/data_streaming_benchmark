package com.org.datastream.count_words_kafka;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class WordReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private boolean complete = false;
    private String str;
    private BufferedReader reader;
    private FileReader fileReader;

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try{
            this.fileReader = new FileReader(conf.get("fileToRead").toString());
        } catch (FileNotFoundException e) {
            new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
        }

        this.collector = spoutOutputCollector;
        this.reader = new BufferedReader(fileReader);
    }

    @Override
    public void nextTuple() {
        if(!complete){
            try{
                System.out.println(str);
                this.str = reader.readLine();
                if(this.str != null){
                    this.collector.emit(new Values(this.str));
                }
                else {
                    complete = true;
                    fileReader.close();
                }
            } catch (IOException e) {
                new RuntimeException("Error reading tuple",e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

}
