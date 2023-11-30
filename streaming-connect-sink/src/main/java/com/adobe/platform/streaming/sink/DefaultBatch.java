package com.adobe.platform.streaming.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public class DefaultBatch<T> implements Batch<T> {
    private List<T> data;
    private int dataSize;

    public DefaultBatch() {
        data = new ArrayList<>();
    }

    @Override
    public void add(T dataToPublish, SinkRecord record, int payloadLength) {
        data.add(dataToPublish);
        dataSize += payloadLength;
    }

    @Override
    public int getSize() {
        return dataSize;
    }

    @Override
    public int getNumberOfEvents() {
        return data.size();
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public List<T> getBatchData() {
        return data;
    }

    @Override
    public void clear() {
        data.clear();
        dataSize = 0;
    }
}
