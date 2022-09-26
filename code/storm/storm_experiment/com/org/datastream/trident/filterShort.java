package com.org.datastream.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class filterShort extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        return tridentTuple.getString(0).length()>3;
    }
}
