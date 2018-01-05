package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.CompiledAggregation;
import com.hazelcast.dataset.DataSet;
import com.hazelcast.mapreduce.aggregation.impl.LongAvgAggregation;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DataSetAggregationTest extends HazelcastTest {

    // properties
    public int keyDomain = 1000;

    private DataSet<Long, Employee> dataSet;
    public String sql = "age = 30 AND active = true";
    public int maxAge = 75;
    public double maxSalary = 1000.0;
    public Class aggregateClass = LongAverageAggregator.class;
    private CompiledAggregation<Object> compiledAggregation;

    @Setup
    public void setUp() throws Exception {
        dataSet = targetInstance.getDataSet(name);
        AggregationRecipe<Object, Object> recipe = new AggregationRecipe<Object, Object>(
                Age.class, ((Aggregator) aggregateClass.newInstance()), new SqlPredicate(sql));
        compiledAggregation = dataSet.compile(recipe);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Long, Employee> streamer = StreamerFactory.getInstance(dataSet);
        Random random = new Random();
        for (long key = 0; key < keyDomain; key++) {
            streamer.pushEntry(key, generateRandomEmployee(random));
        }
        streamer.await();
    }

    private Employee generateRandomEmployee(Random random) {
        int id = random.nextInt();
        int age = random.nextInt(maxAge);
        boolean active = random.nextBoolean();
        double salary = random.nextDouble() * maxSalary;
        return new Employee(id, age, active, salary);
    }

    @TimeStep
    public Object aggregate(ThreadState state) {
        return compiledAggregation.execute(state.bindings);
    }

    public class ThreadState extends BaseThreadState {
        public Map<String, Object> bindings = new HashMap<String, Object>();
    }
}

