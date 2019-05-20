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

package com.hazelcast.simulator.hz.concurrent;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Pipelining;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.concurrent.Executor;

public class AtomicLongTest extends HazelcastTest {

    public int countersLength = 1000;
    public int pipelineDepth = 10;
    public int pipelineIterations = -1;

    private IAtomicLong[] counters;
    private final Executor callerRuns = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };


    @Setup
    public void setup() {
        counters = new IAtomicLong[countersLength];
        if (pipelineIterations == -1) {
            pipelineIterations = pipelineDepth;
        }
        for (int i = 0; i < countersLength; i++) {
            counters[i] = targetInstance.getAtomicLong("" + i);
        }
    }

    @TimeStep(prob = -1)
    public long get(ThreadState state) {
        return state.randomCounter().get();
    }

    @TimeStep(prob = 0.1)
    public long inc(ThreadState state) {
        return state.randomCounter().incrementAndGet();
    }

    @TimeStep(prob = 0)
    public void pipelinedGet(final ThreadState state, final @StartNanos long startNanos, final Probe probe) throws Exception {
        if (state.pipeline == null) {
            state.pipeline = new Pipelining<Long>(pipelineDepth);
        }
        ICompletableFuture<Long> f = state.randomCounter().getAsync();
        f.andThen(new ExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
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

        private IAtomicLong randomCounter() {
            int index = randomInt(counters.length);
            return counters[index];
        }
    }

    @Teardown
    public void teardown() {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
    }
}
