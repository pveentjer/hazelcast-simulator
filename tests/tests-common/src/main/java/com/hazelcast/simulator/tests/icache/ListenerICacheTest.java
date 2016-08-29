/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.IList;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.AfterWarmup;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.icache.helpers.CacheUtils;
import com.hazelcast.simulator.tests.icache.helpers.ICacheEntryEventFilter;
import com.hazelcast.simulator.tests.icache.helpers.ICacheEntryListener;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

/**
 * In this test we add listeners to a cache and record the number of events the listeners receive.
 * We compare those to the number of events we have generated using different cache operations.
 * We verify that no unexpected events have been received.
 */
public class ListenerICacheTest extends AbstractTest {

    private static final int PAUSE_FOR_LAST_EVENTS_SECONDS = 10;

    public int keyCount = 1000;
    public int maxExpiryDurationMs = 500;
    public boolean syncEvents = true;

    private IList<Counter> results;
    private IList<ICacheEntryListener> listeners;

    private ICache<Integer, Long> cache;
    private ICacheEntryListener<Integer, Long> listener;
    private ICacheEntryEventFilter<Integer, Long> filter;
    private final AtomicBoolean afterCompletionCalled = new AtomicBoolean(false);

    @Setup
    public void setup() {
        results = targetInstance.getList(name);
        listeners = targetInstance.getList(name + "listeners");

        cache = CacheUtils.getCache(targetInstance, name);
        listener = new ICacheEntryListener<Integer, Long>();
        filter = new ICacheEntryEventFilter<Integer, Long>();

        CacheEntryListenerConfiguration<Integer, Long> config = new MutableCacheEntryListenerConfiguration<Integer, Long>(
                FactoryBuilder.factoryOf(listener),
                FactoryBuilder.factoryOf(filter),
                false, syncEvents);
        cache.registerCacheEntryListener(config);
    }

    @TimeStep(prob = 0.8, probName = "put")
    public void put(ThreadState state) {
        int key = state.randomInt(keyCount);
        cache.put(key, state.randomLong());
        state.counter.put++;
    }

    @TimeStep(prob = 0, probName = "putExpiry")
    public void putExpiry(ThreadState state) {
        int expiryDuration = state.randomInt(maxExpiryDurationMs);
        ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(new Duration(MILLISECONDS, expiryDuration));

        int key = state.randomInt(keyCount);
        cache.put(key, state.randomLong(), expiryPolicy);
        state.counter.putExpiry++;
    }

    @TimeStep(prob = 0, probName = "putAsyncExpiry")
    public void putAsyncExpiry(ThreadState state) {
        int expiryDuration = state.randomInt(maxExpiryDurationMs);
        ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(new Duration(MILLISECONDS, expiryDuration));

        int key = state.randomInt(keyCount);
        cache.putAsync(key, state.randomLong(), expiryPolicy);
        state.counter.putAsyncExpiry++;
    }

    @TimeStep(prob = 0, probName = "getExpiry")
    public void getExpiry(ThreadState state) {
        int expiryDuration = state.randomInt(maxExpiryDurationMs);
        ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(new Duration(MILLISECONDS, expiryDuration));

        int key = state.randomInt(keyCount);
        cache.get(key, expiryPolicy);
        state.counter.getExpiry++;
    }

    @TimeStep(prob = 0, probName = "getAsyncExpiry")
    public void getAsyncExpiry(ThreadState state) throws Exception {
        int expiryDuration = state.randomInt(maxExpiryDurationMs);
        ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(new Duration(MILLISECONDS, expiryDuration));

        int key = state.randomInt(keyCount);
        Future<Long> future = cache.getAsync(key, expiryPolicy);
        future.get();
        state.counter.getAsyncExpiry++;
    }

    @TimeStep(prob = 0.1, probName = "remove")
    public void remove(ThreadState state) {
        int key = state.randomInt(keyCount);
        if (cache.remove(key)) {
            state.counter.remove++;
        }
    }

    @TimeStep(prob = 0.1, probName = "replace")
    public void replace(ThreadState state) {
        int key = state.randomInt(keyCount);
        if (cache.replace(key, state.randomLong())) {
            state.counter.replace++;
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        results.add(state.counter);

        if (afterCompletionCalled.compareAndSet(false, true)) {
            listeners.add(listener);

            sleepSeconds(PAUSE_FOR_LAST_EVENTS_SECONDS);
        }
    }

    @AfterWarmup
    public void afterWarmup() {
        afterCompletionCalled.set(false);
    }

    public final class ThreadState extends BaseThreadState {

        private final Counter counter = new Counter();
    }

    @Verify(global = false)
    public void localVerify() {
        logger.info(name + " Listener " + listener);
        logger.info(name + " Filter " + filter);
    }

    @Verify
    public void globalVerify() {
        Counter totalCounter = new Counter();
        for (Counter counter : results) {
            totalCounter.add(counter);
        }
        logger.info(name + " " + totalCounter + " from " + results.size() + " Worker threads");

        ICacheEntryListener totalEvents = new ICacheEntryListener();
        for (ICacheEntryListener entryListener : listeners) {
            totalEvents.add(entryListener);
        }
        logger.info(name + " totalEvents: " + totalEvents);
        assertEquals(name + " unexpected events found", 0, totalEvents.getUnexpected());
    }

    private static class Counter implements Serializable {

        public long put;
        public long putExpiry;
        public long putAsyncExpiry;
        public long getExpiry;
        public long getAsyncExpiry;
        public long remove;
        public long replace;

        public void add(Counter counter) {
            put += counter.put;
            putExpiry += counter.putExpiry;
            putAsyncExpiry += counter.putAsyncExpiry;
            getExpiry += counter.getExpiry;
            getAsyncExpiry += counter.getAsyncExpiry;
            remove += counter.remove;
            replace += counter.replace;
        }

        public String toString() {
            return "Counter{"
                    + "put=" + put
                    + ", putExpiry=" + putExpiry
                    + ", putAsyncExpiry=" + putAsyncExpiry
                    + ", getExpiry=" + getExpiry
                    + ", getAsyncExpiry=" + getAsyncExpiry
                    + ", remove=" + remove
                    + ", replace=" + replace
                    + '}';
        }
    }
}
