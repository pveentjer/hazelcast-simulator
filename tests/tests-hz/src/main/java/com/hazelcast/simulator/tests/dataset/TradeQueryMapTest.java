package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

public class TradeQueryMapTest extends HazelcastTest {

    public long maxTradeId = Long.MAX_VALUE;

    public char maxClientId = Character.MAX_VALUE;

    public byte maxVenueCode = Byte.MAX_VALUE;

    public char maxInstrumentCode = Character.MAX_VALUE;

    public int maxPrice = Integer.MAX_VALUE;

    public char maxQuantity = Character.MAX_VALUE;

    public long tradeCount = 10 * 1000 * 1000;

    public String query;

    private IMap<Long, TradeInfo> trades;

    @Setup
    public void setUp() {
        trades = targetInstance.getMap(name);
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

        Streamer<Long, TradeInfo> streamer = StreamerFactory.getInstance(trades, 10000);
        for (long tradeId = 0; tradeId < tradeCount; tradeId++) {
            streamer.pushEntry(tradeId, supplier.get());
        }
        streamer.await();
    }

    @TimeStep
    public Object query() {
        return trades.values(new SqlPredicate(query));
    }
}