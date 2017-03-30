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
package com.hazelcast.simulator.geode;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.ConfigFileTemplate;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.vendors.VendorDriver;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static java.lang.String.format;


/**
 * https://geode.apache.org/docs/guide/basic_config/the_cache/managing_a_peer_server_cache.html
 * https://geode.apache.org/docs/guide/basic_config/the_cache/managing_a_client_cache.html
 */
public class GeodeDriver extends VendorDriver<GemFireCache> {
    private static final Logger LOGGER = Logger.getLogger(GeodeDriver.class);

    private GemFireCache cache;

    @Override
    public WorkerParameters loadWorkerParameters(String workerType, int agentIndex) {
        WorkerParameters params = new WorkerParameters()
                .setAll(properties)
                .set("WORKER_TYPE", workerType)
                .set("file:log4j.xml", loadLog4jConfig());

        if ("member".equals(workerType)) {
            loadServerParameters(params, agents.get(agentIndex - 1));
        } else if ("javaclient".equals(workerType)) {
            loadClientParameters(params);
        } else {
            throw new IllegalArgumentException(format("Unknown workerType [%s]", workerType));
        }

        return params;
    }

    private void loadServerParameters(WorkerParameters params, AgentData agentData) {
        params.set("JVM_OPTIONS", get("MEMBER_ARGS", ""))
                .set("file:geode.xml", loadServerConfig(agentData))
                .set("file:worker.sh", loadWorkerScript("member"));
    }

    private String loadServerConfig(AgentData agentData) {
        String config = loadConfiguration("Geode configuration", "geode.xml");

        StringBuilder sb = new StringBuilder();
        for (AgentData agent : agents) {
            sb.append("<server host=\"").append(agent.getPrivateAddress()).append("\" port=\"" + 6000 + "\"/>");
        }

        ConfigFileTemplate template = new ConfigFileTemplate(config)
                .withAgents(agents)
                .addReplacement("<!--PORT-->", 6000)
                .addReplacement("<!--SERVERS-->", sb)
                .addReplacement("<!--BIND_ADDRESS-->", agentData.getPrivateAddress());
        return template.render();
    }

    private void loadClientParameters(WorkerParameters params) {
        params.set("JVM_OPTIONS", get("CLIENT_ARGS", ""))
                .set("file:geode-client.xml", loadClientConfig())
                .set("file:worker.sh", loadWorkerScript("javaclient"));
    }

    private String loadClientConfig() {
//        String config = loadConfiguration("Geode client configuration", "geode-client.xml");
//
//        ConfigFileTemplate template = new ConfigFileTemplate(config)
//                .withAgents(agents);
//
//        StringBuilder sb = new StringBuilder();
//        for (AgentData agent : agents) {
//            sb.append("<value>").append(agent.getPrivateAddress()).append("</value>");
//        }
//
//        template.addReplacement("<!--ADDRESSES-->", sb.toString());
//        template.addReplacement("<!--CLIENT_MODE-->", client);
//        return template.render();
        throw new RuntimeException();
    }

    @Override
    public GemFireCache getVendorInstance() {
        return cache;
    }

    @Override
    public void startVendorInstance() throws Exception {
        String workerType = get("WORKER_TYPE");

        LOGGER.info(format("%s Geode starting", workerType));
        if ("javaclient".equals(workerType)) {
            ClientCache clientCache = new ClientCacheFactory().create();
            this.cache = clientCache;
        } else {
            File configFile = new File(getUserDir(), "geode.xml");
            Properties properties = new Properties();
            properties.load(new StringReader(FileUtils.fileAsText(configFile)));
            Cache cache = new CacheFactory(properties).set("cache-xml-file", "geode-xml").create();
            this.cache = cache;
        }
        LOGGER.info(format("%s Geode started", workerType));
    }

    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.close();
        }
    }
}
