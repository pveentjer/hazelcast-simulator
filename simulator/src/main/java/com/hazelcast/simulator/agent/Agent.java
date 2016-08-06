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
package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcess;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureMonitor;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureSender;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.common.CoordinatorLogger;
import com.hazelcast.simulator.common.ShutdownThread;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.test.TestSuite;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.test.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.NativeUtils.getPID;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Agent {

    private static final Logger LOGGER = Logger.getLogger(Agent.class);
    private static final AtomicBoolean SHUTDOWN_STARTED = new AtomicBoolean();

    private final File pidFile = new File("agent.pid");

    private final WorkerProcessManager workerProcessManager = new WorkerProcessManager();

    private final int addressIndex;
    private final String publicAddress;
    private final int port;

    private final String cloudProvider;
    private final String cloudIdentity;
    private final String cloudCredential;

    private final WorkerProcessFailureMonitor workerProcessFailureMonitor;
    private final AgentConnector agentConnector;
    private final CoordinatorLogger coordinatorLogger;

    private volatile TestSuite testSuite;

    public Agent(int addressIndex, String publicAddress, int port, String cloudProvider, String cloudIdentity,
                 String cloudCredential, int threadPoolSize, int workerLastSeenTimeoutSeconds) {
        SHUTDOWN_STARTED.set(false);

        this.addressIndex = addressIndex;
        this.publicAddress = publicAddress;
        this.port = port;

        this.cloudProvider = cloudProvider;
        this.cloudIdentity = cloudIdentity;
        this.cloudCredential = cloudCredential;

        this.workerProcessFailureMonitor = new WorkerProcessFailureMonitor(
                new WorkerProcessFailureSenderImpl(),
                workerProcessManager,
                workerLastSeenTimeoutSeconds,
                (int)SECONDS.toMillis(1));
        this.agentConnector = AgentConnector.createInstance(this, workerProcessManager, port, threadPoolSize);
        this.coordinatorLogger = new CoordinatorLogger(agentConnector);

        Runtime.getRuntime().addShutdownHook(new AgentShutdownThread(true));

        createPidFile();

        echo("Simulator Agent is ready for action!");
    }

    private void createPidFile() {
        deleteQuiet(pidFile);
        writeText("" + getPID(), pidFile);
    }

    public int getAddressIndex() {
        return addressIndex;
    }

    public String getPublicAddress() {
        return publicAddress;
    }

    public int getPort() {
        return port;
    }

    public AgentConnector getAgentConnector() {
        return agentConnector;
    }

    public CoordinatorLogger getCoordinatorLogger() {
        return coordinatorLogger;
    }

    public WorkerProcessFailureMonitor getWorkerProcessFailureMonitor() {
        return workerProcessFailureMonitor;
    }

    public void setTestSuite(TestSuite testSuite) {
        this.testSuite = testSuite;
    }

    public TestSuite getTestSuite() {
        return testSuite;
    }

    public File getTestSuiteDir() {
        if (testSuite == null) {
            return null;
        }

        File workersDir = ensureExistingDirectory(getSimulatorHome(), "workers");
        return new File(workersDir, testSuite.getId());
    }

    void start() {
        agentConnector.start();
        workerProcessFailureMonitor.start();
    }

    void shutdown() {
        ShutdownThread thread = new AgentShutdownThread(false);
        thread.start();
        thread.awaitShutdown();
    }

    public static void main(String[] args) {
        try {
            startAgent(args);
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not start Agent!", e);
        }
    }

    static void logHeader() {
        echo("Hazelcast Simulator Agent");
        echo("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime());
        echo("SIMULATOR_HOME: %s%n", getSimulatorHome().getAbsolutePath());

        logImportantSystemProperties();
    }

    static Agent startAgent(String[] args) {
        Agent agent = AgentCli.init(args);
        agent.start();

        echo("CloudIdentity: %s", agent.cloudIdentity);
        echo("CloudCredential: %s", agent.cloudCredential);
        echo("CloudProvider: %s", agent.cloudProvider);

        return agent;
    }

    private static void logImportantSystemProperties() {
        logSystemProperty("java.class.path");
        logSystemProperty("java.home");
        logSystemProperty("java.vendor");
        logSystemProperty("java.vendor.url");
        logSystemProperty("sun.java.command");
        logSystemProperty("java.version");
        logSystemProperty("os.arch");
        logSystemProperty("os.name");
        logSystemProperty("os.version");
        logSystemProperty("user.dir");
        logSystemProperty("user.home");
        logSystemProperty("user.name");
        logSystemProperty("SIMULATOR_HOME");
    }

    private static void logSystemProperty(String name) {
        echo("%s=%s", name, System.getProperty(name));
    }

    private static void echo(String message, Object... args) {
        LOGGER.info(message == null ? "null" : format(message, args));
    }

    private final class WorkerProcessFailureSenderImpl implements WorkerProcessFailureSender {
        private int failureCount;

        @Override
        public boolean send(String message, FailureType type, WorkerProcess workerProcess, String testId, String cause) {
            boolean sentSuccessfully = true;
            boolean isFailure = type != WORKER_FINISHED;
            SimulatorAddress workerAddress = workerProcess.getAddress();
            FailureOperation operation = new FailureOperation(message, type, workerAddress, publicAddress,
                    workerProcess.getHazelcastAddress(), workerProcess.getId(), testId, testSuite, cause);

            if (isFailure) {
                LOGGER.error(format("Detected failure on Worker %s (%s): %s", workerProcess.getId(), workerProcess.getAddress(),
                        operation.getLogMessage(++failureCount)));
            } else {
                LOGGER.info(format("Worker %s (%s) finished.", workerProcess.getId(), workerProcess.getAddress()));
            }

            try {
                Response response = agentConnector.write(SimulatorAddress.COORDINATOR, operation);
                ResponseType firstErrorResponseType = response.getFirstErrorResponseType();
                if (firstErrorResponseType != ResponseType.SUCCESS) {
                    LOGGER.error(format("Could not send failure to coordinator: %s", firstErrorResponseType));
                    sentSuccessfully = false;
                } else if (isFailure) {
                    LOGGER.info("Failure successfully sent to Coordinator!");
                }
            } catch (SimulatorProtocolException e) {
                if (!Thread.currentThread().isInterrupted() && !(e.getCause() instanceof InterruptedException)) {
                    LOGGER.error(format("Could not send failure to coordinator! %s", operation.getFileMessage()), e);
                    sentSuccessfully = false;
                }
            }

            if (type.isWorkerFinishedFailure()) {
                String finishedType = (isFailure) ? "failed" : "finished";
                LOGGER.info(format("Removing %s Worker %s from configuration...", finishedType, workerAddress));
                agentConnector.removeWorker(workerAddress.getWorkerIndex());
            }

            return sentSuccessfully;
        }
    }

    private final class AgentShutdownThread extends ShutdownThread {

        private AgentShutdownThread(boolean ensureProcessShutdown) {
            super("AgentShutdownThread", SHUTDOWN_STARTED, ensureProcessShutdown);
        }

        @Override
        public void doRun() {
            echo("Stopping workers...");
            workerProcessManager.shutdown();

            echo("Stopping WorkerProcessFailureMonitor...");
            workerProcessFailureMonitor.shutdown();

            echo("Stopping AgentConnector...");
            agentConnector.shutdown();

            echo("Removing PID file...");
            deleteQuiet(pidFile);

            OperationTypeCounter.printStatistics();
        }
    }
}
