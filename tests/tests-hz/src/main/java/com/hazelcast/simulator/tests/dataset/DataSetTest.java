package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.dataset.CompiledPredicate;
import com.hazelcast.dataset.DataSet;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataSetTest extends HazelcastTest {

    // properties
    public int keyDomain = 1000 * 1000;
    public String sql = "age = 30 AND active = true";
    private DataSet<Long, Employee> simpleMap;
    private CompiledPredicate<Employee> compiledPredicate;

    @Setup
    public void setup() {
        simpleMap = targetInstance.getDataSet(name);
        compiledPredicate = simpleMap.compile(new SqlPredicate(sql));
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Long, Employee> streamer = StreamerFactory.getInstance(simpleMap);
        for (long k = 0; k < keyDomain; k++) {
            streamer.pushEntry(k, new Employee());
        }
        streamer.await();
    }

    @TimeStep
    public void query(ThreadState state) {
        compiledPredicate.execute(state.newBindings());
    }

    @Verify
    public void verify() {
        assertEquals(keyDomain, simpleMap.size());
    }

    public class ThreadState extends BaseThreadState {
        private Map<String, Object> bindings = new HashMap<String, Object>();

        public Map<String, Object> newBindings() {
            return bindings;
        }
    }
}
