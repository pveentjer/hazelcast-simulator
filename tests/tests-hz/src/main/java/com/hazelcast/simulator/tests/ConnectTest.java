package com.hazelcast.simulator.tests;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.io.File;
import java.io.IOException;


/**
 * A Test that checks how long it takes to establish a connection.
 */
public class ConnectTest extends HazelcastTest {

    private Config memberConfig;
    private ClientConfig clientConfig;

    @Prepare
    public void prepare() throws IOException {
        if (new File("client-hazelcast.xml").exists()) {
            clientConfig = new XmlClientConfigBuilder(new File("client-hazelcast.xml")).build();
        } else {
            memberConfig = new XmlConfigBuilder("hazelcast.xml").build();
        }
        targetInstance.shutdown();
    }

    @TimeStep
    public void test() {
        HazelcastInstance hz = memberConfig == null
                ? HazelcastClient.newHazelcastClient(clientConfig)
                : Hazelcast.newHazelcastInstance(memberConfig);
        hz.shutdown();
    }
}
