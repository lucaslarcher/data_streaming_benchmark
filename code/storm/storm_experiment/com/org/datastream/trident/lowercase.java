package com.org.datastream.trident;

import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class lowercase implements MapFunction {
    @Override
    public Values execute(TridentTuple tridentTuple) {
        return new Values(tridentTuple.getString(0).toLowerCase());
    }
}
