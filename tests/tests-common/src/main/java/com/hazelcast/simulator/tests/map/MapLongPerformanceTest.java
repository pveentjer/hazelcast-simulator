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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.BaseWorkerContext;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

public class MapLongPerformanceTest extends AbstractTest {

    // properties
    public int keyCount = 1000000;
    private IMap<Integer, Long> map;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(basename);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    @Warmup(global = true)
    public void warmup() {
        Streamer<Integer, Long> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            streamer.pushEntry(i, 0L);
        }
        streamer.await();
    }

    @TimeStep
    public void put(BaseWorkerContext context) {
        Integer key = context.randomInt(keyCount);
        map.set(key, System.currentTimeMillis());
    }

    @TimeStep
    public void set(BaseWorkerContext context) {
        Integer key = context.randomInt(keyCount);
        map.set(key, System.currentTimeMillis());
    }

    @TimeStep
    public void get(BaseWorkerContext context) {
        Integer key = context.randomInt(keyCount);
        map.get(key);
    }
}
