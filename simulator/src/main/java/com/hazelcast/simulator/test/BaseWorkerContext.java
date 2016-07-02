package com.hazelcast.simulator.test;

import java.util.Random;

public class BaseWorkerContext {

    private final Random random = new Random();

    public Random getRandom() {
        return random;
    }

    public long randomLong(){
        return random.nextLong();
    }

    public int randomInt(){
        return random.nextInt();
    }

    public int randomInt(int bound){
        return random.nextInt(bound);
    }

    public boolean randomBoolean(){
        return random.nextBoolean();
    }

    public long iteration(){
        return 0;
    }
}
