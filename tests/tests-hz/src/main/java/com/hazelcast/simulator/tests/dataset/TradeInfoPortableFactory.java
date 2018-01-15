package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class TradeInfoPortableFactory  implements PortableFactory {

    public final static int FACTORY_ID = 1;
    public final static int TRADE_INFO = 1;

    @Override
    public Portable create(int classId) {
        if (TRADE_INFO == classId)
            return new TradeInfo();
        else return null;
    }
}