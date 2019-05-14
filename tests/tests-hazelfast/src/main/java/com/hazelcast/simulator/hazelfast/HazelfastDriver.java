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

import com.hazelcast.networktester.Client;
import com.hazelcast.networktester.Server;
import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.vendors.HazelcastDriver;
import com.hazelcast.simulator.vendors.VendorDriver;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

public class HazelfastDriver extends VendorDriver {
    private static final Logger LOGGER = Logger.getLogger(HazelcastDriver.class);

    private Server server;
    private Client client;

    @Override
    public WorkerParameters loadWorkerParameters(String workerType, int agentIndex) {
        Map<String, String> s = new HashMap<String, String>(properties);
        s.remove("CONFIG");

        WorkerParameters params = new WorkerParameters()
                .setAll(s)
                .set("WORKER_TYPE", workerType)
                .set("file:log4j.xml", loadLog4jConfig());

        if ("member".equals(workerType)) {
            loadMemberWorkerParameters(params);
        } else if ("javaclient".equals(workerType)) {
            loadJavaClientWorkerParameters(params);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported workerType [%s]", workerType));
        }

        return params;
    }

    private void loadMemberWorkerParameters(WorkerParameters params) {
        params.set("JVM_OPTIONS", loadJvmOptions("MEMBER_ARGS"))
                .set("file:worker.sh", loadWorkerScript("member"));
    }

    private void loadJavaClientWorkerParameters(WorkerParameters params) {
        params.set("JVM_OPTIONS", loadJvmOptions("CLIENT_ARGS"))
                .set("file:worker.sh", loadWorkerScript("javaclient"));
    }

    private String loadJvmOptions(String argsProperty) {
        return get(argsProperty, "");
    }

    @Override
    public Object getVendorInstance() {
        if (client != null) {
            return client;
        }
        if (server != null) {
            return server;
        }
        return null;
    }


    @Override
    public void startVendorInstance() throws Exception {
        String workerType = get("WORKER_TYPE");

        LOGGER.info(format("%s HazelcastInstance starting", workerType));
        if ("javaclient".equals(workerType)) {
            Client.Context context = new Client.Context().hostname(get("hostname"));
            client = new Client(context);
            client.start();
        } else {
            Server.Context context = new Server.Context().setHostname(get("hostname"));
            server = new Server(context);
            server.start();
        }
        LOGGER.info(format("%s HazelcastInstance started", workerType));
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Stopping HazelcastInstance...");

        if (client != null) {
            client.stop();
            client = null;
        }

        if (server != null) {
            server.stop();
            server = null;
        }
    }
}
