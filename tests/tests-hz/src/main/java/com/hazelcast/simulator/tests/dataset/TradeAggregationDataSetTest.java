package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.DataSet;
import com.hazelcast.dataset.PreparedAggregation;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TradeAggregationDataSetTest extends HazelcastTest {

    // properties
    public long maxTradeId = Long.MAX_VALUE;

    public char maxClientId = Character.MAX_VALUE;

    public byte maxVenueCode = Byte.MAX_VALUE;

    public char maxInstrumentCode = Character.MAX_VALUE;

    public int maxPrice = Integer.MAX_VALUE;

    public char maxQuantity = Character.MAX_VALUE;

    public long tradeCount = 10 * 1000 * 1000;

    public String sqlQuery = "true";

    public Class aggregateClass = LongAverageAggregator.class;
    public boolean useForkJoin = false;
//    public boolean projectionEnabled = false;
//    private Class projectionClass;

    private DataSet<Long, TradeInfo> trades;
    private PreparedAggregation<Object> aggregation;
//    private DataSet<Long, Object> projectedDataset;

    @Setup
    public void setUp() throws Exception {
        trades = targetInstance.getDataSet(name);

        AggregationRecipe<Object, Object> recipe = new AggregationRecipe<>(
                Price.class, ((Aggregator) aggregateClass.newInstance()), new SqlPredicate(sqlQuery));

        aggregation = trades.prepare(recipe);
    }

    @Prepare(global = true)
    public void prepare() {
        TradeInfoSupplier supplier = new TradeInfoSupplier();
        supplier.maxQuantity = maxQuantity;
        supplier.maxPrice = maxPrice;
        supplier.maxInstrumentCode = maxInstrumentCode;
        supplier.maxVenueCode = maxVenueCode;
        supplier.maxClientId = maxClientId;
        supplier.maxTradeId = maxTradeId;
        trades.fill(tradeCount, supplier);
        trades.freeze();
        assertEquals(tradeCount, trades.count());
//
//        if (projectionEnabled) {
//            PreparedProjection p = trades.prepare(new ProjectionRecipe<>(projectionClass, false, new SqlPredicate(sql)));
//            projectedDataset = p.newDataSet("projected", new HashMap<String, Object>());
//
//            AggregationRecipe<Object, Object> recipe = new AggregationRecipe<>(
//                    Age.class, ((Aggregator) aggregateClass.newInstance()), new SqlPredicate(sql));
//
//            aggregation = projectedDataset.prepare(recipe);
//        }
    }

    @TimeStep
    public Object aggregate(ThreadState state) {
        if (useForkJoin) {
            return aggregation.executeForkJoin(state.bindings);
        } else {
            return aggregation.executePartitionThread(state.bindings);
        }
    }

    @Teardown
    public void destroy() {
        logger.info(trades.memoryInfo());
        for (Member member : targetInstance.getCluster().getMembers()) {
            logger.info(member.getAddress());
            for (Partition p : targetInstance.getPartitionService().getPartitions()) {
                if (p.getOwner().equals(member)) {
                    logger.info("      " + p.getPartitionId() + " " + trades.memoryInfo(p.getPartitionId()));
                }
            }
        }
    }

    public class ThreadState extends BaseThreadState {
        public Map<String, Object> bindings = new HashMap<String, Object>();
    }

}
