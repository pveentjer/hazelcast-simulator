package com.hazelcast.simulator.geode.cache;

import com.hazelcast.simulator.geode.GeodeTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.apache.geode.cache.Region;

public class CacheTest extends GeodeTest {

    // properties
    public int keyCount = 10000;

    private Region cache;

    @Setup
    public void setup() {
        cache = gemfireCache.getRegion(name);
    }

    @Prepare(global = true)
    public void prepare() {
        for (int k = 0; k < keyCount; k++) {
            cache.put(k, 0);
        }
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) {
        Integer key = state.randomInt(keyCount);
        cache.put(key, state.value++);
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        Integer key = state.randomInt(keyCount);
        cache.get(key);
    }

    public class ThreadState extends BaseThreadState {
        private int value;
    }

    @Teardown
    public void teardown() {
        cache.close();
    }
}

