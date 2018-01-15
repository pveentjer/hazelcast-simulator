package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.dataset.DataSet;
import com.hazelcast.dataset.PreparedQuery;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class QueryTest extends HazelcastTest {

    // properties
    public int keyDomain = 1000 * 1000;
    public String sql = "age = 30 AND active = true";
    public int maxAge = 75;
    public int maxSalary = 100000;

    private DataSet<Long, Employee> employees;
    private PreparedQuery<Employee> query;

    @Setup
    public void setup() {
        employees = targetInstance.getDataSet(name);
        query = employees.prepare(new SqlPredicate(sql));
    }

    @Prepare(global = true)
    public void prepare() {
        employees.fill(keyDomain, new EmployeeSupplier(maxAge, maxSalary));
        employees.freeze();
    }

    @TimeStep
    public void query(ThreadState state) {
        query.execute(state.bindings);
    }

    @TimeStep(prob = 0)
    public long count() {
        return employees.count();
    }

    @Verify
    public void verify() {
        assertEquals(keyDomain, employees.count());
    }

    @Teardown
    public void destroy() {
        logger.info(employees.memoryInfo());
    }

    public class ThreadState extends BaseThreadState {
        private Map<String, Object> bindings = new HashMap<String, Object>();
    }
}
