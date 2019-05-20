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

package com.hazelcast.simulator.hz.map;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Pipelining;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.spi.impl.SimpleExecutionCallback;

import java.util.Random;
import java.util.concurrent.Executor;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class LongByteArrayMapTest extends HazelcastTest {

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;
    public int pipelineDepth = 10;
    public int pipelineIterations = -1;

    private IMap<Long, byte[]> map;
    private byte[][] values;
    private final Executor callerRuns = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        if (pipelineIterations == -1) {
            pipelineIterations = pipelineDepth;
        }
    }

    @Prepare(global = true)
    public void prepare() {
        Random random = new Random();
        values = new byte[valueCount][];
        for (int i = 0; i < values.length; i++) {
            int delta = minValueLength - maxValueLength;
            int length = delta == 0 ? minValueLength : minValueLength + random.nextInt(delta);
            values[i] = generateByteArray(random, length);
        }

        Streamer<Long, byte[]> streamer = StreamerFactory.getInstance(map);
        for (long key = 0; key < keyDomain; key++) {
            byte[] value = values[random.nextInt(valueCount)];
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep(prob = -1)
    public byte[] get(ThreadState state) {
        return map.get(state.randomKey());
    }

    @TimeStep(prob = -1)
    public void getAsync(ThreadState state, final Probe probe, @StartNanos final long startNanos) {
        map.getAsync(state.randomKey()).andThen(new SimpleExecutionCallback<byte[]>() {
            @Override
            public void notify(Object o) {
                probe.done(startNanos);
            }
        });
    }

    @TimeStep(prob = 0.1)
    public byte[] put(ThreadState state) {
        return map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.0)
    public void putAsync(ThreadState state, final Probe probe, @StartNanos final long startNanos) {
        map.putAsync(state.randomKey(), state.randomValue()).andThen(new SimpleExecutionCallback<byte[]>() {
            @Override
            public void notify(Object o) {
                probe.done(startNanos);
            }
        });
    }

    @TimeStep(prob = 0)
    public void set(ThreadState state) {
        map.set(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0)
    public void setAsync(ThreadState state, final Probe probe, @StartNanos final long startNanos) {
        map.setAsync(state.randomKey(), state.randomValue()).andThen(new SimpleExecutionCallback<Void>() {
            @Override
            public void notify(Object o) {
                probe.done(startNanos);
            }
        });
    }

    @TimeStep(prob = 0)
    public void pipelinedGet(final ThreadState state, final @StartNanos long startNanos, final Probe probe) throws Exception {
        if (state.pipeline == null) {
            state.pipeline = new Pipelining<Long>(pipelineDepth);
        }
        ICompletableFuture<byte[]> f = map.getAsync(state.randomKey());
        f.andThen(new ExecutionCallback<byte[]>() {
            @Override
            public void onResponse(byte[] response) {
                probe.done(startNanos);
            }

            @Override
            public void onFailure(Throwable t) {
                probe.done(startNanos);
            }
        }, callerRuns);
        state.pipeline.add(f);
        state.i++;
        if (state.i == pipelineIterations) {
            state.i = 0;
            state.pipeline.results();
            state.pipeline = null;
        }
    }

    public class ThreadState extends BaseThreadState {
        public int i;
        private Pipelining pipeline;

        private long randomKey() {
            return randomLong(keyDomain);
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
