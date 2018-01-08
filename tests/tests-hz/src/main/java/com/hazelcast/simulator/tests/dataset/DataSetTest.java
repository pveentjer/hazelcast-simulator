package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.dataset.CompiledPredicate;
import com.hazelcast.dataset.DataSet;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class DataSetTest extends HazelcastTest {

    // properties
    public int keyDomain = 1000 * 1000;
    public String sql = "age = 30 AND active = true";
    public int maxAge = 75;
    private DataSet<Long, Employee> dataset;
    private CompiledPredicate<Employee> compiledPredicate;

    @Setup
    public void setup() {
        dataset = targetInstance.getDataSet(name);
        compiledPredicate = dataset.compile(new SqlPredicate(sql));
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Long, Employee> streamer = StreamerFactory.getInstance(dataset);
        Random random = new Random();
        for (long k = 0; k < keyDomain; k++) {
            Employee employee = new Employee();
            employee.age = random.nextInt(maxAge);
            streamer.pushEntry(k, employee);
        }
        streamer.await();
    }

    @TimeStep
    public void query(ThreadState state) {
        compiledPredicate.execute(state.newBindings());
    }

    @TimeStep(prob = 0)
    public long count(ThreadState state) {
        return dataset.count();
    }


    @Verify
    public void verify() {
        assertEquals(keyDomain, dataset.count());
    }

    @Teardown
    public void destroy(){
        System.out.println(dataset.memoryUsage());
    }


    public class ThreadState extends BaseThreadState {
        private Map<String, Object> bindings = new HashMap<String, Object>();

        public Map<String, Object> newBindings() {
            return bindings;
        }
    }
}
