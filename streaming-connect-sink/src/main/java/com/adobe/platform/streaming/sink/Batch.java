package com.adobe.platform.streaming.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public interface Batch<T> {
    void add(T dataToPublish, SinkRecord record, int payloadLength);

    int getSize();

    int getNumberOfEvents();

    boolean isEmpty();

    List<T> getBatchData();

    void clear();
}
