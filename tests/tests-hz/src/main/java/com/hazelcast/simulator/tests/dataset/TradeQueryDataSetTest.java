package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.dataset.DataSet;
import com.hazelcast.dataset.PreparedQuery;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.HashMap;
import java.util.Map;

public class TradeQueryDataSetTest extends HazelcastTest {

    public long maxTradeId = Long.MAX_VALUE;

    public char maxClientId = Character.MAX_VALUE;

    public byte maxVenueCode = Byte.MAX_VALUE;

    public char maxInstrumentCode = Character.MAX_VALUE;

    public int maxPrice = Integer.MAX_VALUE;

    public char maxQuantity = Character.MAX_VALUE;

    public long tradeCount = 10 * 1000 * 1000;

    public String query;

    private DataSet<Long, TradeInfo> trades;
    private PreparedQuery<TradeInfo> preparedQuery;

    @Setup
    public void setUp() {
        trades = targetInstance.getDataSet(name);
        preparedQuery = trades.prepare(new SqlPredicate(query));
    }

    @Prepare
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
    }

    @TimeStep
    public Object query(ThreadState state){
        return preparedQuery.execute(state.bindings);
    }

    public class ThreadState extends BaseThreadState {
        public Map<String, Object> bindings = new HashMap<String, Object>();
    }
}