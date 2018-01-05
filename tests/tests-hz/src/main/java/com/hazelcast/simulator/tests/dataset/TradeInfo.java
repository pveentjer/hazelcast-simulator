package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class TradeInfo implements DataSerializable {

    public long tradeId;

    // the person making the thread
    public int clientId;

    // venue is a particular trading platform, e.g. the NY stock exchange
    // venue code can be used to route on
    public int venueCode;

    // the code of the tradeable asset. E.g. gold
    public int instrumentCode;

    // the total price of the trade.
    public long price;

    // the number of items to trade
    public long quantity;

    // buy or sell.
    public char side;

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(tradeId);
        out.writeInt(clientId);
        out.writeInt(venueCode);
        out.writeInt(instrumentCode);
        out.writeLong(price);
        out.writeLong(quantity);
        out.writeChar(side);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.tradeId = in.readLong();
        this.clientId = in.readInt();
        this.venueCode = in.readInt();
        this.instrumentCode = in.readInt();
        this.price = in.readLong();
        this.quantity = in.readLong();
        this.side = in.readChar();
    }
}
