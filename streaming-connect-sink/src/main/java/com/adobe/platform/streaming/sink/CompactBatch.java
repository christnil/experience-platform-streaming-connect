package com.adobe.platform.streaming.sink;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CompactBatch<T> implements Batch<T> {
    private List<Object> keysOrdered;
    private Map<Object, Pair<Integer, T>> data;
    private int dataSize;

    public CompactBatch() {
        keysOrdered = new ArrayList<>();
        data = new HashMap<>();
    }

    @Override
    public void add(T dataToPublish, SinkRecord record, int payloadLength) {
        if (data.containsKey(record.key())) {
            keysOrdered.remove(record.key());
            dataSize -= data.get(record.key()).getLeft();
            data.remove(record.key());
        }
        data.put(record.key(), Pair.of(payloadLength, dataToPublish));
        keysOrdered.add(record.key());
        dataSize += payloadLength;
    }

    @Override
    public int getSize() {
        return dataSize;
    }

    @Override
    public int getNumberOfEvents() {
        return keysOrdered.size();
    }

    @Override
    public boolean isEmpty() {
        return keysOrdered.isEmpty();
    }

    @Override
    public List<T> getBatchData() {
        return keysOrdered.stream().map(key -> data.get(key).getRight()).collect(Collectors.toList());
    }

    @Override
    public void clear() {
        keysOrdered.clear();
        data.clear();
        dataSize = 0;
    }
}
