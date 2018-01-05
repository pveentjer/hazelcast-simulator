package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.aggregation.impl.LongSumAggregator;
import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.CompiledAggregation;
import com.hazelcast.dataset.DataSet;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.HashMap;
import java.util.Map;

public class TradeDataSetTest extends HazelcastTest {

    // properties

    // the person making the thread
    public int clientCount = 100;

    // venue is a particular trading platform, e.g. the NY stock exchange
    // venue code can be used to route on
    public int venueCount = 100;

    // the code of the tradeable asset. E.g. gold
    public int instrumentCount = 1000;

    // the total price of the trade.
    public long maxPrice = 1000;

    // the number of items to trade
    public long maxQuantity = 1000;

    private DataSet<Integer, TradeInfo> trades;
    private CompiledAggregation<Object> totalBuySum;

    @Setup
    public void setUp()  {
        trades = targetInstance.getDataSet(name);

        totalBuySum = trades.compile(new AggregationRecipe<Object, Object>(Price.class, new LongSumAggregator(), new SqlPredicate("buy=true")));
    }

    @TimeStep(executionGroup = "insert")
    public void insert(ThreadState state){
        TradeInfo tradeInfo = state.generateTrade();
        trades.insert(tradeInfo.venueCode, tradeInfo);
    }

    @TimeStep(executionGroup = "query")
    public Object totalBuySum(ThreadState state){
        return totalBuySum.execute(state.bindings);
    }

    public class ThreadState extends BaseThreadState {
        public Map<String, Object> bindings = new HashMap<String, Object>();

        private TradeInfo generateTrade() {
            TradeInfo tradeInfo = new TradeInfo();
            tradeInfo.clientId = randomInt(clientCount);
            tradeInfo.venueCode = randomInt(venueCount);
            tradeInfo.instrumentCode = randomInt(instrumentCount);
            tradeInfo.price = randomLong(maxPrice);
            tradeInfo.quantity = randomLong(maxQuantity);
            tradeInfo.side = randomBoolean()?'b':'s';
            return tradeInfo;
        }
    }
}