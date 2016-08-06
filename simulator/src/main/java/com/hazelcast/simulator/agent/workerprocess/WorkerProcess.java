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

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.BuildInfoUtils.getHazelcastVersionFromJAR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.directoryForVersionSpec;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Represents a worker process. So the process that does the actual work.
 */
public class WorkerProcess {
    public static final String WORKERS_HOME_NAME = "workers";

    private static final int WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS = 500;

    private static final String CLASSPATH = System.getProperty("java.class.path");
    private static final String CLASSPATH_SEPARATOR = System.getProperty("path.separator");

    private static final Logger LOGGER = Logger.getLogger(WorkerProcess.class);

    private final AtomicBoolean javaHomePrinted = new AtomicBoolean();

    private volatile SimulatorAddress address;
    private volatile String id;
    private volatile File workerHome;

    private volatile long lastSeen = System.currentTimeMillis();
    private volatile boolean oomeDetected;
    private volatile boolean isFinished;
    private volatile Process process;
    private volatile String hzAddress;

    private final Agent agent;
    private final WorkerProcessManager workerProcessManager;
    private final WorkerProcessSettings workerProcessSettings;

    private File hzConfigFile;
    private File log4jFile;
    private File testSuiteDir;

    public WorkerProcess(Agent agent,
                         WorkerProcessManager workerProcessManager,
                         WorkerProcessSettings workerProcessSettings) {
        this.agent = agent;
        this.workerProcessManager = workerProcessManager;
        this.workerProcessSettings = workerProcessSettings;
    }

    public SimulatorAddress getAddress() {
        return address;
    }

    public String getId() {
        return id;
    }

    public File getWorkerHome() {
        return workerHome;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void updateLastSeen() {
        this.lastSeen = System.currentTimeMillis();
    }

    public boolean isOomeDetected() {
        return oomeDetected;
    }

    public void setOomeDetected() {
        this.oomeDetected = true;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void setFinished() {
        isFinished = true;
    }

    public Process getProcess() {
        return process;
    }

    public String getHazelcastAddress() {
        return hzAddress;
    }

    public void start() {
        try {
            testSuiteDir = agent.getTestSuiteDir();
            ensureExistingDirectory(testSuiteDir);

            WorkerType type = workerProcessSettings.getWorkerType();
            int workerIndex = workerProcessSettings.getWorkerIndex();
            LOGGER.info(format("Starting a Java Virtual Machine for %s Worker #%d", type, workerIndex));

            LOGGER.info("Starting Worker using settings: " + workerProcessSettings);
            start0();
            LOGGER.info(format("Finished starting a for %s Worker #%d", type, workerIndex));

            waitForStartup();
        } catch (Exception e) {
            throw new WorkerProcessFailedToStartException("Failed to start Worker", e);
        }
    }

    private void start0() throws IOException {
        int workerIndex = workerProcessSettings.getWorkerIndex();
        WorkerType type = workerProcessSettings.getWorkerType();

        address = new SimulatorAddress(
                AddressLevel.WORKER, agent.getAddressIndex(), workerIndex, 0);
        id = address.toString() + '-' + agent.getPublicAddress() + '-' + type.toLowerCase();
        workerHome = ensureExistingDirectory(testSuiteDir, id);

        copyResourcesToWorkerHome(id);

        String hzConfigFileName = (type == WorkerType.MEMBER) ? "hazelcast" : "client-hazelcast";
        hzConfigFile = ensureExistingFile(workerHome, hzConfigFileName + ".xml");
        writeText(workerProcessSettings.getHazelcastConfig(), hzConfigFile);

        log4jFile = ensureExistingFile(workerHome, "log4j.xml");
        writeText(workerProcessSettings.getLog4jConfig(), log4jFile);

        generateWorkerStartScript();

        ProcessBuilder processBuilder = new ProcessBuilder(new String[]{"bash", "worker.sh"})
                .directory(workerHome);

        Map<String, String> environment = processBuilder.environment();
        String javaHome = getJavaHome();
        String path = javaHome + "/bin:" + environment.get("PATH");
        environment.put("PATH", path);
        environment.put("JAVA_HOME", javaHome);

        this.process = processBuilder.start();
        workerProcessManager.add(address, this);
    }

    private void waitForStartup() {
        int workerStartupTimeout = workerProcessSettings.getWorkerStartupTimeout();
        int loopCount = (int) SECONDS.toMillis(workerStartupTimeout) / WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS;
        for (int i = 0; i < loopCount; i++) {
            if (hasExited()) {
                throw new WorkerProcessFailedToStartException(format(
                        "Startup of Worker %s on Agent %s failed, check log files in %s for more information!",
                        address, agent.getPublicAddress(), workerHome));
            }

            String address = readAddress();
            if (address != null) {
                this.hzAddress = address;
                LOGGER.info(format("Worker %s started", id));
                return;
            }

            sleepMillis(WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS);
        }

        throw new WorkerProcessFailedToStartException(format(
                "Worker %s on Agent %s didn't start within %s seconds, check log files in %s for more information!",
                address, agent.getPublicAddress(), workerStartupTimeout, workerHome));
    }

    private String getJavaHome() {
        String javaHome = System.getProperty("java.home");
        if (javaHomePrinted.compareAndSet(false, true)) {
            LOGGER.info("java.home=" + javaHome);
        }
        return javaHome;
    }

    private void generateWorkerStartScript() {
        File startScript = new File(getWorkerHome(), "worker.sh");

        String script = workerProcessSettings.getWorkerScript();
        script = replaceAll(script, "CLASSPATH", getClasspath());
        script = replaceAll(script, "JVM_OPTIONS", workerProcessSettings.getJvmOptions());
        script = replaceAll(script, "LOG4J_FILE", log4jFile.getAbsolutePath());
        script = replaceAll(script, "SIMULATOR_HOME", getSimulatorHome());
        script = replaceAll(script, "WORKER_ID", getId());
        script = replaceAll(script, "WORKER_TYPE", workerProcessSettings.getWorkerType());
        script = replaceAll(script, "PUBLIC_ADDRESS", agent.getPublicAddress());
        script = replaceAll(script, "AGENT_INDEX", agent.getAddressIndex());
        script = replaceAll(script, "WORKER_INDEX", workerProcessSettings.getWorkerIndex());
        script = replaceAll(script, "WORKER_PORT", agent.getPort() + workerProcessSettings.getWorkerIndex());
        script = replaceAll(script, "AUTO_CREATE_HZ_INSTANCE", workerProcessSettings.isAutoCreateHzInstance());
        script = replaceAll(script, "WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                workerProcessSettings.getPerformanceMonitorIntervalSeconds());
        script = replaceAll(script, "HZ_CONFIG_FILE", hzConfigFile.getAbsolutePath());

        writeText(script, startScript);
    }

    private String replaceAll(String script, String variable, Object value) {
        return script.replaceAll("@" + variable, "" + value);
    }

    private void copyResourcesToWorkerHome(String workerId) {
        File workerHome = new File(getSimulatorHome(), WORKERS_HOME_NAME);
        String testSuiteId = agent.getTestSuite().getId();
        File uploadDirectory = new File(workerHome, testSuiteId + "/upload/").getAbsoluteFile();
        if (!uploadDirectory.exists() || !uploadDirectory.isDirectory()) {
            LOGGER.debug("Skip copying upload directory to workers since no upload directory was found");
            return;
        }
        String copyCommand = format("cp -rfv %s/%s/upload/* %s/%s/%s/",
                workerHome,
                testSuiteId,
                workerHome,
                testSuiteId,
                workerId);
        execute(copyCommand);
        LOGGER.info(format("Finished copying '%s' to Worker", workerHome));
    }

    private boolean hasExited() {
        try {
            process.exitValue();
            return true;
        } catch (IllegalThreadStateException e) {
            return false;
        }
    }

    private String readAddress() {
        File file = new File(workerHome, "worker.address");
        if (!file.exists()) {
            return null;
        }

        String address = fileAsText(file);
        deleteQuiet(file);

        return address;
    }

    private String getClasspath() {
        String simulatorHome = getSimulatorHome().getAbsolutePath();
        String hzVersionDirectory = directoryForVersionSpec(workerProcessSettings.getHazelcastVersionSpec());
        String testJarVersion = getHazelcastVersionFromJAR(simulatorHome + "/hz-lib/" + hzVersionDirectory + "/*");
        LOGGER.info(format("Adding Hazelcast %s and test JARs %s to classpath", hzVersionDirectory, testJarVersion));

        // we have to reverse the classpath to monkey patch version specific classes
        return new File(agent.getTestSuiteDir(), "lib/*").getAbsolutePath()
                + CLASSPATH_SEPARATOR + simulatorHome + "/user-lib/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/test-lib/" + testJarVersion + "/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/test-lib/common/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/hz-lib/" + hzVersionDirectory + "/*"
                + CLASSPATH_SEPARATOR + CLASSPATH;
    }
}
