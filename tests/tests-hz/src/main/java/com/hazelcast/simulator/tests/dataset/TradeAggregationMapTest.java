package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Bits;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class TradeAggregationMapTest extends HazelcastTest {
    // properties
    public long maxTradeId = Long.MAX_VALUE;

    public char maxClientId = Character.MAX_VALUE;

    public byte maxVenueCode = Byte.MAX_VALUE;

    public char maxInstrumentCode = Character.MAX_VALUE;

    public int maxPrice = Integer.MAX_VALUE;

    public char maxQuantity = Character.MAX_VALUE;

    public long tradeCount = 10 * 1000 * 1000;

    public String sqlQuery = "true";

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
    public Object aggregate() {
        if (sqlQuery.equals("true")) {
            return trades.aggregate(Aggregators.<Map.Entry<Long, TradeInfo>>longMax("price"));
        } else {
            return trades.aggregate(Aggregators.<Map.Entry<Long, TradeInfo>>longMax("price"), new SqlPredicate(sqlQuery));
        }
    }

    @Teardown
    public void tearDown() throws Exception {
        long memoryBytes = 0;

        Map<Member, Future<Long>> futures = targetInstance
                .getExecutorService("foo")
                .submitToAllMembers(new HeapCostCallable(name));

        for (Future<Long> f : futures.values()) {
            memoryBytes += f.get();
        }

        logger.info("trades.size:"+trades.size());
        logger.info("trades count:"+tradeCount);
        logger.info("Total memorycost: " + memoryBytes+" bytes");
        int payloadSize = trades.size() * (TradeInfo.PAYLOAD_SIZE_BYTES + Bits.LONG_SIZE_IN_BYTES);
        logger.info("Total payload bytes: " + payloadSize);
        logger.info("Memory Efficiency: " + ((payloadSize * 100d) / memoryBytes) + "%");
        logger.info("Map.Entry payload size: " + (TradeInfo.PAYLOAD_SIZE_BYTES + Bits.LONG_SIZE_IN_BYTES) + " bytes");
        logger.info("Payload size: " + (memoryBytes / tradeCount) + " bytes");
    }

    public static class HeapCostCallable implements Callable<Long>, HazelcastInstanceAware, Serializable {

        private final String mapName;
        private transient HazelcastInstance hz;

        public HeapCostCallable(String mapName) {
            this.mapName = mapName;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }

        @Override
        public Long call() throws Exception {
            return hz.getMap(mapName).getLocalMapStats().getOwnedEntryMemoryCost();
        }
    }
}
