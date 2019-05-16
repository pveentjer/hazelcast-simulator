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
package com.hazelcast.simulator.hazelfast;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelfast.Client;

import java.io.IOException;
import java.util.Random;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class DummyHazelfastTest extends HazelfastTest {

    // properties
    public int keyCount = 1000;
    public int keyLength = 10;
    public int pipelineDepth = 5;
    private byte[][] keys;

    @Setup
    public void setUp() {
        Random random = new Random();
        keys = new byte[keyCount][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateByteArray(random, keyLength);
        }
    }

    @TimeStep(prob = 1)
    public void get(ThreadState state) throws IOException {
        state.client.writeAndFlush(state.randomKey());
        state.client.readResponse();
    }

    @TimeStep(prob = 0)
    public void pipedLinedGet(ThreadState state, Probe probe, @StartNanos long startNanos) throws IOException {
        if (state.writeIndex < pipelineDepth) {
            state.startNanoArray[state.writeIndex] = startNanos;
            state.client.write(state.randomKey());
            state.writeIndex++;
            if (state.writeIndex == pipelineDepth) {
                state.client.flush();
            }
        } else {
            state.client.readResponse();
            long startNs = state.startNanoArray[state.readIndex];
            probe.done(startNs);
            state.readIndex++;
            if (state.readIndex == pipelineDepth) {
                state.writeIndex = 0;
                state.readIndex = 0;
            }
        }
    }

    public class ThreadState extends BaseThreadState {

        private byte[] randomKey() {
            return keys[randomInt(keys.length)];
        }

        private int writeIndex;
        private int readIndex;
        private long[] startNanoArray = new long[pipelineDepth];

        Client client;

        public ThreadState() {
            try {
                Client.Context context = new Client.Context().hostname(DummyHazelfastTest.this.client.hostname());
                client = new Client(context);
                client.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
