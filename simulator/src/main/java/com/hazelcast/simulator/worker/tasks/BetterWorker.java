package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BetterWorker implements IWorker {

    private final List<Method> beforeRunMethods;
    private final List<Method> afterRunMethods;
    private final List<Method> timeStepMethods;
    @InjectTestContext
    private TestContext testContext;

    private Object testInstance;
    private Object workerContext;

    public BetterWorker(Object testInstance) {
        this.beforeRunMethods = getMethodsAnnotatedWith(testInstance.getClass(), BeforeRun.class);
        this.afterRunMethods = getMethodsAnnotatedWith(testInstance.getClass(), AfterRun.class);
        this.timeStepMethods = getMethodsAnnotatedWith(testInstance.getClass(), TimeStep.class);
    }

    @Override
    public void beforeRun() throws Exception {
        for (Method beforeRunMethod : beforeRunMethods) {
            int argCount = beforeRunMethod.getParameterTypes().length;
            switch (argCount) {
                case 0:
                    beforeRunMethod.invoke(testInstance);
                    break;
                case 1:
                    beforeRunMethod.invoke(testInstance, getOrCreateWorkerContext(beforeRunMethod.getParameterTypes()[0]));
                    break;
                default:
                    throw new RuntimeException();
            }
        }
    }

    private Object getOrCreateWorkerContext(Class workerContextType) throws Exception {
        if (workerContext == null) {
            Constructor constructor = null;
            try {
                constructor = workerContextType.getConstructor();
            } catch (NoSuchMethodException e) {
            }

            try {
                constructor = workerContextType.getConstructor(testInstance.getClass());
            } catch (NoSuchMethodException e) {
            }

            if (constructor == null) {
                throw new RuntimeException("No constructor found for class:" + workerContextType)
            }

            constructor.setAccessible(true);

            if (constructor.getTypeParameters().length == 0) {
                workerContext = constructor.newInstance();
            } else {
                workerContext = constructor.newInstance(testContext);
            }
        }

        return workerContext;
    }

    @Override
    public void run() throws Exception {
        while (!testContext.isStopped()) {
            for (Method timeStepMethod : timeStepMethods) {
                int argCount = timeStepMethod.getParameterTypes().length;
                switch (argCount) {
                    case 0:
                        timeStepMethod.invoke(testInstance);
                        break;
                    case 1:
                        timeStepMethod.invoke(testInstance, getOrCreateWorkerContext(timeStepMethod.getParameterTypes()[0]));
                        break;
                    default:
                        throw new RuntimeException();
                }
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        for (Method afterRunMethod : afterRunMethods) {
            int argCount = afterRunMethod.getParameterTypes().length;
            switch (argCount) {
                case 0:
                    afterRunMethod.invoke(testInstance);
                    break;
                case 1:
                    afterRunMethod.invoke(testInstance, getOrCreateWorkerContext(afterRunMethod.getParameterTypes()[0]));
                    break;
                default:
                    throw new RuntimeException();
            }
        }
    }

    @Override
    public void afterCompletion() throws Exception {

    }

    public static List<Method> getMethodsAnnotatedWith(final Class<?> type, final Class<? extends Annotation> annotation) {
        final List<Method> methods = new ArrayList<Method>();
        Class<?> clazz = type;
        while (clazz != Object.class) { // need to iterated thought hierarchy in order to retrieve methods from above the current instance
            // iterate though the list of methods declared in the class represented by clazz variable, and add those annotated with the specified annotation
            final List<Method> allMethods = new ArrayList<Method>(Arrays.asList(clazz.getDeclaredMethods()));
            for (final Method method : allMethods) {
                if (method.isAnnotationPresent(annotation)) {
                    Annotation annotInstance = method.getAnnotation(annotation);
                    // TODO process annotInstance
                    methods.add(method);
                }
            }
            // move to the upper class in the hierarchy in search for more methods
            clazz = clazz.getSuperclass();
        }
        return methods;
    }
}
