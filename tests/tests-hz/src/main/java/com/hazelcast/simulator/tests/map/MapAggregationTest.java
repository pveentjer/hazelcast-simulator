package com.hazelcast.simulator.tests.map;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.helpers.DataSerializableEmployee;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Map;
import java.util.Random;

public class MapAggregationTest extends HazelcastTest {

    // properties
    public int keyDomain = 1000;

    private IMap<Long, DataSerializableEmployee> map;
    public String sql = "age = 30 AND active = true";
    public int maxAge = 75;
    public double maxSalary = 1000.0;
    private boolean selectAll;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        selectAll = sql.trim().equals("true");
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Long, DataSerializableEmployee> streamer = StreamerFactory.getInstance(map);
        Random random = new Random();
        for (long key = 0; key < keyDomain; key++) {
            streamer.pushEntry(key, generateRandomEmployee(random));
        }
        streamer.await();
    }

    private DataSerializableEmployee generateRandomEmployee(Random random) {
        int id = random.nextInt();
        int age = random.nextInt(maxAge);
        boolean active = random.nextBoolean();
        double salary = random.nextDouble() * maxSalary;
        return new DataSerializableEmployee(id, age, active, salary);
    }

    @TimeStep
    public Object aggregate() {
        if (selectAll) {
            return map.aggregate(new AverageSalaryAggregator());
        } else {
            return map.aggregate(new AverageSalaryAggregator(),new SqlPredicate(sql));
        }
    }

    public static class AverageSalaryAggregator extends Aggregator<Map.Entry<Long, DataSerializableEmployee>, Double> {
        protected long sum;
        protected long count;

        @Override
        public void accumulate(Map.Entry<Long, DataSerializableEmployee> entry) {
            count++;
            sum += entry.getValue().getSalary();
        }

        @Override
        public void combine(Aggregator aggregator) {
            this.sum += this.getClass().cast(aggregator).sum;
            this.count += this.getClass().cast(aggregator).count;
        }

        @Override
        public Double aggregate() {
            if (count == 0) {
                return null;
            }
            return ((double) sum / (double) count);
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}

