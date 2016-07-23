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
package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.PerformanceStateOperation;
import com.hazelcast.simulator.protocol.operation.TestHistogramOperation;
import com.hazelcast.simulator.test.TestContainer;
import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.worker.performance.PerformanceState.INTERVAL_LATENCY_PERCENTILE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Monitors the performance of all running Simulator Tests on {@link com.hazelcast.simulator.worker.MemberWorker}
 * and {@link com.hazelcast.simulator.worker.ClientWorker} instances.
 */
public class WorkerPerformanceMonitor {

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 10;
    private static final long WAIT_FOR_TEST_CONTAINERS_DELAY_NANOS = MILLISECONDS.toNanos(100);
    private static final Logger LOGGER = Logger.getLogger(WorkerPerformanceMonitor.class);

    private final WorkerPerformanceMonitorThread thread;
    private final AtomicBoolean shutdown = new AtomicBoolean();

    public WorkerPerformanceMonitor(ServerConnector serverConnector, Collection<TestContainer> testContainers,
                                    int workerPerformanceMonitorInterval, TimeUnit workerPerformanceIntervalTimeUnit) {
        long intervalNanos = workerPerformanceIntervalTimeUnit.toNanos(workerPerformanceMonitorInterval);
        this.thread = new WorkerPerformanceMonitorThread(serverConnector, testContainers, intervalNanos);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOGGER.fatal(e.getMessage(), e);
            }
        });
    }

    public void start() {
        thread.start();
    }

    public void shutdown() throws InterruptedException {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }

        thread.join(MINUTES.toMillis(SHUTDOWN_TIMEOUT_SECONDS));
    }

    /**
     * Thread to monitor the performance of Simulator Tests.
     * <p>
     * Iterates over all {@link TestContainer} to retrieve performance values from all {@link Probe} instances.
     * Sends performance numbers as {@link PerformanceState} to the Coordinator.
     * Writes performance stats to files.
     * <p>
     * Holds one {@link TestPerformanceTracker} instance per Simulator Test.
     */
    private final class WorkerPerformanceMonitorThread extends Thread {

        private final PerformanceStatsWriter globalPerformanceStatsWriter;
        private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        private final Map<String, TestPerformanceTracker> trackers = new ConcurrentHashMap<String, TestPerformanceTracker>();
        private final ServerConnector serverConnector;
        private final Collection<TestContainer> testContainers;
        private final long intervalNanos;

        private WorkerPerformanceMonitorThread(ServerConnector serverConnector,
                                               Collection<TestContainer> testContainers,
                                               long intervalNanos) {
            super("WorkerPerformanceMonitor");
            setDaemon(true);
            this.serverConnector = serverConnector;
            this.testContainers = testContainers;
            this.intervalNanos = intervalNanos;
            this.globalPerformanceStatsWriter = new PerformanceStatsWriter(new File("performance.csv"));
        }

        @Override
        public void run() {
            while (!shutdown.get()) {
                long startedNanos = System.nanoTime();
                long currentTimestamp = System.currentTimeMillis();

                boolean runningTestFound = refreshTests(currentTimestamp);
                updateTrackers(currentTimestamp);
                sendPerformanceStates();
                writeStatsToFiles(currentTimestamp);
                purgeDeadTrackers(currentTimestamp);

                long elapsedNanos = System.nanoTime() - startedNanos;
                if (intervalNanos > elapsedNanos) {
                    if (runningTestFound) {
                        sleepNanos(intervalNanos - elapsedNanos);
                    } else {
                        sleepNanos(WAIT_FOR_TEST_CONTAINERS_DELAY_NANOS - elapsedNanos);
                    }
                } else {
                    LOGGER.warn("WorkerPerformanceMonitorThread.run() took " + NANOSECONDS.toMillis(elapsedNanos) + " ms");
                }
            }
            sendTestHistograms();
        }

        private void sendTestHistograms() {
            for (Map.Entry<String, TestPerformanceTracker> entry : trackers.entrySet()) {
                String testId = entry.getKey();
                TestPerformanceTracker tracker = entry.getValue();

                Map<String, String> histograms = tracker.aggregateIntervalHistograms();
                if (!histograms.isEmpty()) {
                    TestHistogramOperation operation = new TestHistogramOperation(testId, histograms);
                    serverConnector.write(SimulatorAddress.COORDINATOR, operation);
                }
            }
        }

        private boolean refreshTests(long currentTimestamp) {
            boolean runningTestFound = false;

            for (TestContainer testContainer : testContainers) {
                if (!testContainer.isRunning()) {
                    continue;
                }

                String testId = testContainer.getTestContext().getTestId();
                TestPerformanceTracker tracker = trackers.get(testId);
                if (tracker == null) {
                    tracker = new TestPerformanceTracker(testContainer);
                    trackers.put(testId, tracker);
                }

                // we set the lastSeen timestamp, so we can easily purge dead trackers
                tracker.lastSeen = currentTimestamp;
                runningTestFound = true;
            }

            return runningTestFound;
        }


        // we remove every MonitoredTest that doesn't have the desired timestamp
        private void purgeDeadTrackers(long currentTimestamp) {
            for (TestPerformanceTracker tracker : trackers.values()) {
                // purge the testData if it wasn't seen in the current run
                if (tracker.lastSeen == currentTimestamp) {
                    continue;
                }

                trackers.remove(tracker.testId);

                // we need to make sure the histogram data gets written on deletion.
                Map<String, String> histograms = tracker.aggregateIntervalHistograms();
                if (!histograms.isEmpty()) {
                    TestHistogramOperation operation = new TestHistogramOperation(tracker.testId, histograms);
                    serverConnector.write(SimulatorAddress.COORDINATOR, operation);
                }
            }
        }

        private void updateTrackers(long currentTimestamp) {
            for (TestPerformanceTracker tracker : trackers.values()) {
                updateTrackers(currentTimestamp, tracker);
            }
        }

        private void updateTrackers(long currentTimestamp, TestPerformanceTracker tracker) {
            TestContainer testContainer = tracker.testContainer;
            Map<String, Probe> probeMap = testContainer.getProbeMap();
            Map<String, Histogram> intervalHistograms = new HashMap<String, Histogram>(probeMap.size());

            long intervalPercentileLatency = -1;
            double intervalMean = -1;
            long intervalMaxLatency = -1;

            long oldIterations = tracker.oldIterations;
            long iterations = testContainer.iteration();
            tracker.oldIterations = iterations;
            long intervalOperationCount = iterations - oldIterations;

            for (Map.Entry<String, Probe> entry : probeMap.entrySet()) {
                String probeName = entry.getKey();
                Probe probe = entry.getValue();

                if (!(probe instanceof HdrProbe)) {
                    continue;
                }

                HdrProbe hdrProbe = (HdrProbe) probe;
                Histogram intervalHistogram = hdrProbe.getIntervalHistogram();
                intervalHistograms.put(probeName, intervalHistogram);

                long percentileValue = intervalHistogram.getValueAtPercentile(INTERVAL_LATENCY_PERCENTILE);
                if (percentileValue > intervalPercentileLatency) {
                    intervalPercentileLatency = percentileValue;
                }

                double meanLatency = intervalHistogram.getMean();
                if (meanLatency > intervalMean) {
                    intervalMean = meanLatency;
                }

                long maxValue = intervalHistogram.getMaxValue();
                if (maxValue > intervalMaxLatency) {
                    intervalMaxLatency = maxValue;
                }

                if (probe.isPartOfTotalThroughput()) {
                    intervalOperationCount += intervalHistogram.getTotalCount();
                }
            }

            tracker.update(
                    intervalHistograms,
                    intervalPercentileLatency,
                    intervalMean,
                    intervalMaxLatency,
                    intervalOperationCount,
                    currentTimestamp);
        }

        private void sendPerformanceStates() {
            PerformanceStateOperation operation = new PerformanceStateOperation();

            for (TestPerformanceTracker tracker : trackers.values()) {
                if (tracker.isUpdated()) {
                    operation.addPerformanceState(tracker.testId, tracker.createPerformanceState());
                }
            }

            if (operation.getPerformanceStates().size() > 0) {
                serverConnector.submit(SimulatorAddress.COORDINATOR, operation);
            }
        }

        private void writeStatsToFiles(long currentTimestamp) {
            if (trackers.isEmpty()) {
                return;
            }

            String dateString = simpleDateFormat.format(new Date(currentTimestamp));
            long globalIntervalOperationCount = 0;
            long globalOperationsCount = 0;
            double globalIntervalThroughput = 0;

            for (TestPerformanceTracker tracker : trackers.values()) {
                if (tracker.getAndResetIsUpdated()) {
                    tracker.writeStatsToFile(currentTimestamp, dateString);

                    globalIntervalOperationCount += tracker.getIntervalOperationCount();
                    globalOperationsCount += tracker.getTotalOperationCount();
                    globalIntervalThroughput += tracker.getIntervalThroughput();
                }
            }

            // global performance stats
            globalPerformanceStatsWriter.write(
                    currentTimestamp,
                    dateString,
                    globalOperationsCount,
                    globalIntervalOperationCount,
                    globalIntervalThroughput,
                    trackers.size(),
                    testContainers.size());
        }
    }
}
