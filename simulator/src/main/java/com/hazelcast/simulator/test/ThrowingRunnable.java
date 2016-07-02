package com.hazelcast.simulator.test;

public interface ThrowingRunnable  {

    final ThrowingRunnable NO_OP = new ThrowingRunnable() {
        @Override
        public void run() throws Exception {
        }
    };

    void run() throws Exception;
}
