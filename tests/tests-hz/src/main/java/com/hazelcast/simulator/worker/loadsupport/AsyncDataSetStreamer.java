package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.dataset.DataSet;

import java.util.concurrent.Semaphore;

public class AsyncDataSetStreamer<K, V> extends AbstractAsyncStreamer<K, V> {

    private final DataSet<K, V> dataSet;

    AsyncDataSetStreamer(int concurrencyLevel, DataSet<K, V> dataSet) {
        super(concurrencyLevel);
        this.dataSet = dataSet;
    }

    AsyncDataSetStreamer(int concurrencyLevel, DataSet<K, V> dataSet, Semaphore semaphore) {
        super(concurrencyLevel, semaphore);
        this.dataSet = dataSet;
    }

    @Override
    ICompletableFuture storeAsync(K key, V value) {
        return dataSet.insertAsync(key, value);
    }
}