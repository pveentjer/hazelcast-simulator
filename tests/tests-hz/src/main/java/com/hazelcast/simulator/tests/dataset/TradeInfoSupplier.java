package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.function.Supplier;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class TradeInfoSupplier implements Supplier<TradeInfo>, DataSerializable{

    public long maxTradeId;

    public char maxClientId;

    public byte maxVenueCode;

    public char maxInstrumentCode;

    public int maxPrice;

    public char maxQuantity;

    private final TradeInfo tradeInfo = new TradeInfo();

    @Override
    public TradeInfo get() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        tradeInfo.tradeId = random.nextLong(maxTradeId);
        tradeInfo.price = random.nextInt(maxPrice);
        //tradeInfo.clientId = random.next
       //tradeInfo.maxVenueCode = random.nextB();

        return tradeInfo;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(maxTradeId);
        out.writeChar(maxClientId);
        out.writeByte(maxVenueCode);
        out.writeChar(maxInstrumentCode);
        out.writeInt(maxPrice);
        out.writeChar(maxQuantity);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        maxTradeId = in.readLong();
        maxClientId = in.readChar();
        maxVenueCode = in.readByte();
        maxInstrumentCode = in.readChar();
        maxPrice = in.readInt();
        maxQuantity = in.readChar();
    }
}
