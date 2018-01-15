package com.hazelcast.simulator.tests.dataset;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * A TradeInfo contains exactly 20 bytes of content.
 */
public class TradeInfo implements Portable {

    public final static int PAYLOAD_SIZE_BYTES = 20;

    public long tradeId;

    // the person making the thread
    public char clientId;

    // venue is a particular trading platform, e.g. the NY stock exchange
    // venue code can be used to route on
    public byte venueCode;

    // the code of the tradeable asset. E.g. gold
    public char instrumentCode;

    // the total price of the trade.
    public int price;

    // the number of items to trade
    public char quantity;

    // if it is a buy or sell
    public boolean side;

    @Override
    public int getFactoryId() {
        return TradeInfoPortableFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TradeInfoPortableFactory.TRADE_INFO;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeLong("tradeId", tradeId);
        writer.writeChar("clientId", clientId);
        writer.writeByte("venueCode", venueCode);
        writer.writeChar("instrumentCode", instrumentCode);
        writer.writeInt("price", price);
        writer.writeChar("quantity", quantity);
        writer.writeBoolean("side", side);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        tradeId = reader.readLong("tradeId");
        clientId = reader.readChar("clientId");
        venueCode = reader.readByte("venueCode");
        instrumentCode = reader.readChar("instrumentCode");
        price = reader.readInt("price");
        quantity = reader.readChar("quantity");
        side = reader.readBoolean("side");
    }
}
