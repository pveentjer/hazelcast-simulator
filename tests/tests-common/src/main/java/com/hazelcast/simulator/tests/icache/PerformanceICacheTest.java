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

import com.hazelcast.simulator.test.BaseWorkerContext;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * A performance test for the cache. The key is integer and value is a integer
 */
public class PerformanceICacheTest extends AbstractTest {

    // properties
    public int keyCount = 1000000;
    public double putProb = 0.1;

    private Cache<Object, Object> cache;

    @Setup
    public void setup() {
        CacheManager cacheManager = createCacheManager(targetInstance);
        cache = cacheManager.getCache(basename);
    }

    @Teardown
    public void teardown() {
        cache.close();
    }

    @Warmup(global = true)
    public void warmup() {
        Streamer<Object, Object> streamer = StreamerFactory.getInstance(cache);
        for (int i = 0; i < keyCount; i++) {
            streamer.pushEntry(i, 0);
        }
        streamer.await();
    }

    @TimeStep
    public void put(WorkerContext context) {
        Integer key = context.randomInt(keyCount);
        cache.put(key, context.value++);
    }

    @TimeStep
    public void get(WorkerContext context) {
        Integer key = context.randomInt(keyCount);
        cache.get(key);
    }

    private class WorkerContext extends BaseWorkerContext {
        private int value;
    }
}
