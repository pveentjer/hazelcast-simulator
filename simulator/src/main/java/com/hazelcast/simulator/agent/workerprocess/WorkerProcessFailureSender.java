package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.test.FailureType;

public interface WorkerProcessFailureSender {

    boolean send(String message, FailureType type, WorkerProcess workerProcess, String testId, String cause);
}
