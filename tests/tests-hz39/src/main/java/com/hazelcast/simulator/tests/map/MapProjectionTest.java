package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class MapProjectionTest extends AbstractTest {

    // properties
    public int keyCount = 1000;
    public int valueCount = 1000;
    public int minSize = 16;
    public int maxSize = 2000;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int projectionDelayUs;
    public int bytesToReturn = 1000;

    private IMap<Integer, Object> map;
    private int[] keys;
    private byte[][] values;


    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);

        if (minSize > maxSize) {
            throw new IllegalStateException("minSize can't be larger than maxSize");
        }
    }

    @Prepare
    public void prepare() {
        Random random = new Random();
        values = new byte[valueCount][];
        for (int i = 0; i < values.length; i++) {
            int delta = maxSize - minSize;
            int length = delta == 0 ? minSize : minSize + random.nextInt(delta);
            values[i] = generateByteArray(random, length);
        }

        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(map);
        for (int key : keys) {
            streamer.pushEntry(key, values[random.nextInt(values.length)]);
        }
        streamer.await();
    }

    @TimeStep
    public void get(ThreadState state) {
        map.get(state.randomKey(), new MyProjection(projectionDelayUs, bytesToReturn));
    }

    public static class MyProjection extends Projection implements DataSerializable {
        private int projectionDelayUs;
        private int bytesToReturn;

        public MyProjection() {
        }

        public MyProjection(int projectionDelayUs, int bytesToReturn) {
            this.projectionDelayUs = projectionDelayUs;
            this.bytesToReturn = bytesToReturn;
        }

        @Override
        public Object transform(Object o) {
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(projectionDelayUs));

            byte[] result = new byte[bytesToReturn];
            byte[] bytes = (byte[]) o;
            System.arraycopy(bytes, 0, result, 0, result.length);
            return bytes;
        }

        @Override
        public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
            objectDataOutput.writeInt(projectionDelayUs);
            objectDataOutput.writeInt(bytesToReturn);
        }

        @Override
        public void readData(ObjectDataInput objectDataInput) throws IOException {
            projectionDelayUs = objectDataInput.readInt();
            bytesToReturn = objectDataInput.readInt();
        }
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}

