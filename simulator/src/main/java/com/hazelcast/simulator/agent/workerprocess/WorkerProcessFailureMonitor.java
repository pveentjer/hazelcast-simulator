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
package com.hazelcast.simulator.agent.workerprocess;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Date;

import static com.hazelcast.simulator.test.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.test.FailureType.WORKER_EXIT;
import static com.hazelcast.simulator.test.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.test.FailureType.WORKER_OOM;
import static com.hazelcast.simulator.test.FailureType.WORKER_TIMEOUT;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class WorkerProcessFailureMonitor {

    private static final Logger LOGGER = Logger.getLogger(WorkerProcessFailureMonitor.class);

    private final MonitorThread monitorThread;
    private final WorkerProcessFailureSender failureListener;

    public WorkerProcessFailureMonitor(
            WorkerProcessFailureSender failureSender,
            WorkerProcessManager processManager,
            int lastSeenTimeoutSeconds,
            int scanIntervalMillis) {
        this.failureListener = failureSender;
        this.monitorThread = new MonitorThread(processManager, lastSeenTimeoutSeconds, scanIntervalMillis);
    }

    public void start() {
        monitorThread.start();
    }

    public void shutdown() {
        monitorThread.running = false;
        monitorThread.interrupt();
    }

    public void startTimeoutDetection() {
        if (monitorThread.lastSeenTimeoutSeconds > 0) {
            LOGGER.info("Starting timeout detection for Workers...");
            monitorThread.updateLastSeen();
            monitorThread.detectTimeouts = true;
        }
    }

    public void stopTimeoutDetection() {
        if (monitorThread.lastSeenTimeoutSeconds > 0) {
            LOGGER.info("Stopping timeout detection for Workers...");
            monitorThread.detectTimeouts = false;
        }
    }

    private final class MonitorThread extends Thread {

        private final WorkerProcessManager workerProcessManager;
        private final int lastSeenTimeoutSeconds;
        private final int scanIntervalMillis;

        private volatile boolean running = true;
        private volatile boolean detectTimeouts;

        private MonitorThread(WorkerProcessManager workerProcessManager, int lastSeenTimeoutSeconds,
                              int scanIntervalMillis) {
            super("WorkerJvmFailureMonitorThread");
            setDaemon(true);

            this.workerProcessManager = workerProcessManager;
            this.lastSeenTimeoutSeconds = lastSeenTimeoutSeconds;
            this.scanIntervalMillis = scanIntervalMillis;
        }

        @Override
        public void run() {
            while (running) {
                for (WorkerProcess workerProcess : workerProcessManager.getWorkerProcesses()) {
                    //System.out.println("lastSeen:" + new Date(workerProcess.getLastSeen()));
                    try {
                        detectFailures(workerProcess);
                    } catch (Exception e) {
                        LOGGER.fatal("Failed to scan for failures", e);
                    }
                }

                sleepMillis(scanIntervalMillis);
            }
        }

        private void updateLastSeen() {
            for (WorkerProcess workerProcess : workerProcessManager.getWorkerProcesses()) {
                workerProcess.updateLastSeen();
            }
        }

        private void detectFailures(WorkerProcess workerProcess) {
            if (workerProcess.isFinished()) {
                return;
            }

            detectExceptions(workerProcess);
            if (workerProcess.isOomeDetected()) {
                return;
            }

            detectOomeFailure(workerProcess);
            detectInactivity(workerProcess);
            detectUnexpectedExit(workerProcess);
        }

        private void detectExceptions(WorkerProcess workerProcess) {
            File workerHome = workerProcess.getWorkerHome();
            if (!workerHome.exists()) {
                return;
            }

            File[] exceptionFiles = ExceptionExtensionFilter.listFiles(workerHome);
            for (File exceptionFile : exceptionFiles) {
                String content = fileAsText(exceptionFile);

                int indexOf = content.indexOf(NEW_LINE);
                String testId = content.substring(0, indexOf);
                String cause = content.substring(indexOf + 1);

                if (testId.isEmpty() || "null".equals(testId)) {
                    testId = null;
                }

                // we delete or rename the exception file so that we don't detect the same exception again
                boolean send = failureListener.send(
                        "Worked ran into an unhandled exception", WORKER_EXCEPTION, workerProcess, testId, cause);

                if (send) {
                    deleteQuiet(exceptionFile);
                } else {
                    rename(exceptionFile, new File(exceptionFile.getName() + ".sendFailure"));
                }
            }
        }

        private void detectOomeFailure(WorkerProcess workerProcess) {
            if (!isOomeFound(workerProcess.getWorkerHome())) {
                return;
            }
            workerProcess.setOomeDetected();

            failureListener.send("Worker ran into an OOME", WORKER_OOM, workerProcess, null, null);
        }

        private boolean isOomeFound(File workerHome) {
            File oomeFile = new File(workerHome, "worker.oome");
            if (oomeFile.exists()) {
                return true;
            }

            // if we find the hprof file, we also know there is an OOME. The problem with the worker.oome file is that it is
            // created after the heap dump is done, and creating the heap dump can take a lot of time. And then the system could
            // think there is another problem (e.g. lack of inactivity; or timeouts). This hides the OOME.
            File[] hprofFiles = HProfExtensionFilter.listFiles(workerHome);
            return (hprofFiles.length > 0);
        }

        private void detectInactivity(WorkerProcess workerProcess) {
            if (!detectTimeouts) {
                return;
            }

            long idleTimeSeconds = MILLISECONDS.toSeconds(System.currentTimeMillis() - workerProcess.getLastSeen());

            if (idleTimeSeconds > 0 && idleTimeSeconds % lastSeenTimeoutSeconds == 0) {
                String msg = format("Worker has not sent a message for %d seconds", idleTimeSeconds);
                failureListener.send(msg, WORKER_TIMEOUT, workerProcess, null, null);
            }
        }

        private void detectUnexpectedExit(WorkerProcess workerProcess) {
            Process process = workerProcess.getProcess();
            int exitCode;
            try {
                exitCode = process.exitValue();
            } catch (IllegalThreadStateException ignore) {
                // process is still running
                return;
            }

            if (exitCode == 0) {
                workerProcess.setFinished();
                failureListener.send("Worker terminated normally", WORKER_FINISHED, workerProcess, null, null);
            } else {
                workerProcessManager.shutdown(workerProcess);
                String msg = format("Worker terminated with exit code %d instead of 0", exitCode);
                failureListener.send(msg, WORKER_EXIT, workerProcess, null, null);
            }
        }
    }

    static class ExceptionExtensionFilter implements FilenameFilter {

        private static final ExceptionExtensionFilter INSTANCE = new ExceptionExtensionFilter();
        private static final File[] EMPTY_FILES = new File[0];

        static File[] listFiles(File workerHome) {
            File[] exceptionFiles = workerHome.listFiles(ExceptionExtensionFilter.INSTANCE);
            if (exceptionFiles == null) {
                return EMPTY_FILES;
            }
            return exceptionFiles;
        }

        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".exception");
        }
    }

    static class HProfExtensionFilter implements FilenameFilter {

        private static final HProfExtensionFilter INSTANCE = new HProfExtensionFilter();
        private static final File[] EMPTY_FILES = new File[0];

        static File[] listFiles(File workerHome) {
            File[] hprofFiles = workerHome.listFiles(HProfExtensionFilter.INSTANCE);
            if (hprofFiles == null) {
                return EMPTY_FILES;
            }
            return hprofFiles;
        }

        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".hprof");
        }
    }
}
