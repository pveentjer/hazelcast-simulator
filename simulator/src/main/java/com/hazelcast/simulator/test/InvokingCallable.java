package com.hazelcast.simulator.test;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

public class InvokingCallable implements Callable {
    private final Object instance;
    private final Method method;
    private final Object[] args;

    public InvokingCallable(Object instance, Method method, Object... args) {
        this.instance = instance;
        this.method = method;
        this.args = args;
    }

    @Override
    public Object call() throws Exception {
        return method.invoke(instance, args);
    }
}
