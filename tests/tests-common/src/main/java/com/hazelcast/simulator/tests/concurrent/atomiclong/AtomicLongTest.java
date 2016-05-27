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
package com.hazelcast.simulator.tests.concurrent.atomiclong;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.Partition;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationThread;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getPartitionDistributionInformation;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static org.junit.Assert.assertEquals;

public class AtomicLongTest {

    private static final ILogger LOGGER = Logger.getLogger(AtomicLongTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public String basename = AtomicLongTest.class.getSimpleName();
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int countersLength = 1000;
    public int warmupIterations = 100;

    public double writeProb = 1.0;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private HazelcastInstance targetInstance;
    private IAtomicLong totalCounter;
    private IAtomicLong[] counters;

    @Setup
    public void setup(TestContext testContext) {
        targetInstance = testContext.getTargetInstance();

        totalCounter = targetInstance.getAtomicLong(basename + ":TotalCounter");
        counters = new IAtomicLong[countersLength];

        String[] names = generateStringKeys(basename, countersLength, keyLocality, testContext.getTargetInstance());
        for (int i = 0; i < countersLength; i++) {
            counters[i] = targetInstance.getAtomicLong(names[i]);
        }

        builder.addOperation(Operation.PUT, writeProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void teardown() throws Exception {
        display();

        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
        totalCounter.destroy();
        LOGGER.info(getOperationCountInformation(targetInstance));
        LOGGER.info(getPartitionDistributionInformation(targetInstance));
    }

    @Warmup
    public void warmup() {
        for (int i = 0; i < warmupIterations; i++) {
            for (IAtomicLong counter : counters) {
                counter.get();
            }
        }
    }

    @Verify
    public void verify() {

        String serviceName = totalCounter.getServiceName();
        String totalName = totalCounter.getName();

        long actual = 0;
        for (DistributedObject distributedObject : targetInstance.getDistributedObjects()) {
            String key = distributedObject.getName();
            if (serviceName.equals(distributedObject.getServiceName()) && key.startsWith(basename) && !key.equals(totalName)) {
                actual += targetInstance.getAtomicLong(key).get();
            }
        }

        assertEquals(totalCounter.get(), actual);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private int increments;

        public Worker() {
            super(builder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            IAtomicLong counter = getRandomCounter();

            switch (operation) {
                case PUT:
                    increments++;
                    counter.incrementAndGet();
                    break;
                case GET:
                    counter.get();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public void afterRun() {
            totalCounter.addAndGet(increments);
        }

        private IAtomicLong getRandomCounter() {
            int index = randomInt(counters.length);
            return counters[index];
        }
    }

    public void display() throws Exception {
        List<Partition> localPartitions = new LinkedList<Partition>();
        for (Partition partition : targetInstance.getPartitionService().getPartitions()) {
            if (targetInstance.getCluster().getLocalMember().equals(partition.getOwner())) {
                localPartitions.add(partition);
            }
        }

        OperationServiceImpl operationServiceImpl = (OperationServiceImpl) getNode(targetInstance).nodeEngine.getOperationService();
        OperationExecutorImpl executor = (OperationExecutorImpl) operationServiceImpl.getOperationExecutor();

        PartitionOperationThread[] partitionThreads = getPartitionOperationThreads(executor);

        Map<PartitionOperationThread, Integer> partitionsPerThread = new HashMap<PartitionOperationThread, Integer>();
        for (Partition localPartition : localPartitions) {
            int index = localPartition.getPartitionId() % partitionThreads.length;
            PartitionOperationThread thread = partitionThreads[index];
            Integer count = partitionsPerThread.get(thread);
            if (count == null) {
                count = 0;
            }
            partitionsPerThread.put(thread, count + 1);
        }

        long completedByAllThreads = getTotalCompletedCount(partitionThreads);

        double desiredLoad = 100d / partitionThreads.length;
        LOGGER.info("Expected share: " + desiredLoad + "%");

        double totalDifference = 0;
        for (PartitionOperationThread thread : partitionThreads) {
            int partitionCount = partitionsPerThread.get(thread) == null ? 0 : partitionsPerThread.get(thread);

            long completedByThread = getTotalCompletedCount(thread);
            double actualLoad = 100d * completedByThread / completedByAllThreads;

            totalDifference += Math.abs(actualLoad - desiredLoad);
            LOGGER.info(String.format("PartitionThread-%d"
                    + " partitions: %d"
                    + " tasks: %d"
                    + " actual load: %4.3f%", thread.getId(), partitionCount, completedByThread, actualLoad));
        }

        LOGGER.info("Total difference : " + totalDifference + "%");

    }

    private PartitionOperationThread[] getPartitionOperationThreads(OperationExecutorImpl executor) throws NoSuchFieldException, IllegalAccessException {
        Field field = OperationExecutorImpl.class.getDeclaredField("partitionThreads");
        field.setAccessible(true);
        return (PartitionOperationThread[]) field.get(executor);
    }

    private long getTotalCompletedCount(PartitionOperationThread thread) throws NoSuchFieldException, IllegalAccessException {
        Field field = OperationThread.class.getDeclaredField("completedTotalCount");
        field.setAccessible(true);
        return ((SwCounter) field.get(thread)).get();
    }

    private long getTotalCompletedCount(PartitionOperationThread[] threads) throws NoSuchFieldException, IllegalAccessException {
        long result = 0;
        for (PartitionOperationThread thread : threads) {
            result += getTotalCompletedCount(thread);
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        AtomicLongTest test = new AtomicLongTest();
        new TestRunner<AtomicLongTest>(test).withDuration(10).run();
    }
}
