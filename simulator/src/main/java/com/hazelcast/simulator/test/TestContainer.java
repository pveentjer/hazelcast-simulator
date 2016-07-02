/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.probes.impl.ThroughputProbe;
import com.hazelcast.simulator.test.annotations.InjectHazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.AnnotationFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.TeardownFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.VerifyFilter;
import com.hazelcast.simulator.utils.AnnotationFilter.WarmupFilter;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.MetronomeType;
import com.hazelcast.simulator.worker.tasks.IMultipleProbesWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;
import org.apache.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.test.TestPhase.LOCAL_WARMUP;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodSkipArgsCheck;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getMetronomeIntervalMillis;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getMetronomeType;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getProbeName;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.isPartOfTotalThroughput;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.getPropertyValue;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokeMethod;
import static com.hazelcast.simulator.utils.ReflectionUtils.setFieldValue;
import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedIntervalMs;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.NOP;
import static com.hazelcast.simulator.worker.tasks.IWorker.DEFAULT_WORKER_PROBE_NAME;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.text.WordUtils.capitalizeFully;

/**
 * Container for test class instances.
 * <p>
 * <ul>
 * <li>Creates the test class instance by its fully qualified class name.</li>
 * <li>Binds properties to the test class instance (test parameters).</li>
 * <li>Injects required objects to annotated fields.</li>
 * <li>Analyses the test class instance for annotated test phase methods.</li>
 * <li>Provides a method to invoke test methods.</li>
 * </ul>
 */
public class TestContainer {

    static final String THREAD_COUNT_PROPERTY_NAME = "threadCount";
    static final String METRONOME_INTERVAL_PROPERTY_NAME = "metronomeInterval";
    static final String METRONOME_TYPE_PROPERTY_NAME = "metronomeType";
    static final String LIGHTWEIGHT_PROBE_PROPERTY_NAME = "lightweightProbe";

    private static final int DEFAULT_RUN_WITH_WORKER_THREAD_COUNT = 10;
    private static final int DEFAULT_RUN_WITH_WORKER_METRONOME_INTERVAL = 0;
    private static final MetronomeType DEFAULT_RUN_WITH_WORKER_METRONOME_TYPE = NOP;
    private static final boolean DEFAULT_IS_LIGHTWEIGHT_PROBE = false;

    private static final Set<String> OPTIONAL_TEST_PROPERTIES = new HashSet<String>(asList(
            THREAD_COUNT_PROPERTY_NAME,
            METRONOME_INTERVAL_PROPERTY_NAME,
            METRONOME_TYPE_PROPERTY_NAME,
            LIGHTWEIGHT_PROBE_PROPERTY_NAME
    ));

    private static final Logger LOGGER = Logger.getLogger(TestContainer.class);

    private final Map<String, Probe> probeMap = new ConcurrentHashMap<String, Probe>();

    private final TestContext testContext;
    private final Object testInstance;
    private final Class testClassType;

    private final int runWithWorkerThreadCount;
    private final int runWithWorkerMetronomeInterval;
    private final MetronomeType runWithWorkerMetronomeType;
    private final boolean runWithWorkerIsLightweightProbe;

    private long testStartedTimestamp;
    private volatile boolean isRunning;

    private final Map<TestPhase, ThrowingRunnable> taskPerPhaseMap = new HashMap<TestPhase, ThrowingRunnable>();

    public TestContainer(TestContext testContext, TestCase testCase) {
        this(testContext, getTestClassInstance(testCase), getThreadCount(testCase),
                getMetronomeIntervalProperty(testCase), getMetronomeTypeProperty(testCase),
                isLightweightProbe(testCase));
    }

    public TestContainer(TestContext testContext, Object testInstance) {
        this(testContext, testInstance, DEFAULT_RUN_WITH_WORKER_THREAD_COUNT,
                DEFAULT_RUN_WITH_WORKER_METRONOME_INTERVAL, DEFAULT_RUN_WITH_WORKER_METRONOME_TYPE, DEFAULT_IS_LIGHTWEIGHT_PROBE);
    }

    public TestContainer(TestContext testContext, Object testInstance, boolean runWithWorkerIsLightweightProbe) {
        this(testContext, testInstance, DEFAULT_RUN_WITH_WORKER_THREAD_COUNT,
                DEFAULT_RUN_WITH_WORKER_METRONOME_INTERVAL, DEFAULT_RUN_WITH_WORKER_METRONOME_TYPE,
                runWithWorkerIsLightweightProbe);
    }

    public TestContainer(TestContext testContext, Object testInstance, int runWithWorkerThreadCount) {
        this(testContext, testInstance, runWithWorkerThreadCount,
                DEFAULT_RUN_WITH_WORKER_METRONOME_INTERVAL, DEFAULT_RUN_WITH_WORKER_METRONOME_TYPE, DEFAULT_IS_LIGHTWEIGHT_PROBE);
    }

    public TestContainer(TestContext testContext, Object testInstance, int runWithWorkerThreadCount,
                         int runWithWorkerMetronomeInterval, MetronomeType runWithWorkerMetronomeType,
                         boolean runWithWorkerIsLightweightProbe) {
        if (testContext == null) {
            throw new NullPointerException("testContext cannot be null!");
        }
        if (testInstance == null) {
            throw new NullPointerException("testInstance cannot be null!");
        }

        this.testContext = testContext;
        this.testInstance = testInstance;
        this.testClassType = testInstance.getClass();

        this.runWithWorkerThreadCount = runWithWorkerThreadCount;
        this.runWithWorkerMetronomeInterval = runWithWorkerMetronomeInterval;
        this.runWithWorkerMetronomeType = runWithWorkerMetronomeType;
        this.runWithWorkerIsLightweightProbe = runWithWorkerIsLightweightProbe;


        injectDependencies();
        initTestMethods();
    }

    public Object getTestInstance() {
        return testInstance;
    }

    public TestContext getTestContext() {
        return testContext;
    }

    public long getTestStartedTimestamp() {
        return testStartedTimestamp;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public Map<String, Probe> getProbeMap() {
        return probeMap;
    }

    public void invoke(TestPhase testPhase) throws Exception {
        ThrowingRunnable task = taskPerPhaseMap.get(testPhase);
        if (task != null) {
            task.run();
        }
    }

    // just for testing
    boolean hasProbe(String probeName) {
        return probeMap.containsKey(probeName);
    }

    private void injectDependencies() {
        Map<Field, Object> injectMap = createInjectMap(testClassType);
        injectObjects(injectMap, testInstance);
    }

    private void initTestMethods() {
        final Method setupMethod = getAtMostOneVoidMethodSkipArgsCheck(testClassType, Setup.class);
        if (setupMethod != null) {
            setupTask = new ThrowingRunnable() {
                @Override
                public void run() throws Exception {
                    setupMethod.invoke()
                }
            }
            setupArguments = getSetupArguments(setupMethod);
            testMethods.put(TestPhase.SETUP, setupMethod);
        }

        Method localWarmupMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Warmup.class, new WarmupFilter(false));
        if (localWarmupMethod != null) {
            taskPerPhaseMap.put(LOCAL_WARMUP, new InvokingCallable(testInstance, localWarmupMethod))
        }

        Method globalWarmupMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Warmup.class, new WarmupFilter(true));
        if (localWarmupMethod != null) {
            globalWarmupTask = new InvokingCallable(testInstance, globalWarmupMethod);
        }

        Method localVerifyMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Verify.class, new VerifyFilter(false));
        if (localVerifyMethod != null) {
            localVerifyTask = new InvokingCallable(testInstance, localVerifyMethod);
        }

        Method globalVerifyMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Verify.class, new VerifyFilter(true));
        if (globalVerifyMethod != null) {
            globalVerifyTask = new InvokingCallable(testInstance, globalVerifyMethod);
        }


        Method localTearDownMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Verify.class, new TeardownFilter(false));
        if (localTearDownMethod != null) {
            localTearDownTask = new InvokingCallable(testInstance, localTearDownMethod);
        }

        Method globalTeardownMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Verify.class, new TeardownFilter(true));
        if (globalTeardownMethod != null) {
            globalTearDownTask = new InvokingCallable(testInstance, globalTeardownMethod);
        }

        Method runMethod;
        Method runWithWorkerMethod;
        try {
            runMethod = getAtMostOneVoidMethodWithoutArgs(testClassType, Run.class);
            runWithWorkerMethod = getAtMostOneMethodWithoutArgs(testClassType, RunWithWorker.class, IWorker.class);


            if (runWithWorkerMethod != null) {
                runWithWorker = true;
                testMethods.put(TestPhase.RUN, runWithWorkerMethod);
            } else if (runMethod != null) {
                testMethods.put(TestPhase.RUN, runMethod);
            } else {
                testMethods.put(TestPhase.RUN, )
            }


            extractTestMethod(Warmup.class, new WarmupFilter(false), TestPhase.LOCAL_WARMUP);
            extractTestMethod(Warmup.class, new WarmupFilter(true), TestPhase.GLOBAL_WARMUP);

            extractTestMethod(Verify.class, new VerifyFilter(false), TestPhase.LOCAL_VERIFY);
            extractTestMethod(Verify.class, new VerifyFilter(true), TestPhase.GLOBAL_VERIFY);

            extractTestMethod(Teardown.class, new TeardownFilter(false), TestPhase.LOCAL_TEARDOWN);
            extractTestMethod(Teardown.class, new TeardownFilter(true), TestPhase.GLOBAL_TEARDOWN);
        } catch (Exception e) {
            throw new IllegalTestException("Error during search for annotated test methods in" + testClassType.getName(), e);
        }
//        if ((runMethod == null) == (runWithWorkerMethod == null)) {
//            throw new IllegalTestException(format("Test must contain either %s or %s method", Run.class, RunWithWorker.class));
//        }
    }

    private Object[] getSetupArguments(Method setupMethod) {
        Class[] parameterTypes = setupMethod.getParameterTypes();
        Object[] arguments = new Object[parameterTypes.length];
        if (parameterTypes.length < 1) {
            return arguments;
        }

        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            if (!parameterType.isAssignableFrom(TestContext.class) || parameterType.isAssignableFrom(Object.class)) {
                throw new IllegalTestException(format("Method %s.%s() supports arguments of type %s, but found %s at position %d",
                        testClassType.getSimpleName(), setupMethod, TestContext.class.getName(), parameterType.getName(), i));
            }
            arguments[i] = testContext;
        }
        return arguments;
    }

    private void extractTestMethod(Class<? extends Annotation> annotationClass, AnnotationFilter filter, TestPhase testPhase) {
        Method method = getAtMostOneVoidMethodWithoutArgs(testClassType, annotationClass, filter);
        testMethods.put(testPhase, method);
    }

    private void invokeRun() throws Exception {
        try {
            Method method = testMethods.get(TestPhase.RUN);
            if (runWithWorker) {
                runWithRunAnnotation(method);
            } else {
                testStartedTimestamp = System.currentTimeMillis();
                isRunning = true;
                invokeMethod(testInstance, method);
            }
        } finally {
            isRunning = false;
        }
    }

    private void runWithRunAnnotation(Method runMethod) throws Exception {
        LOGGER.info(format("Spawning %d worker threads for test %s", runWithWorkerThreadCount, testContext.getTestId()));
        if (runWithWorkerThreadCount <= 0) {
            return;
        }

        // create instance to get the class of the IWorker implementation
        IWorker workerInstance = invokeMethod(testInstance, runMethod);
        Class<? extends IWorker> workerClass = workerInstance.getClass();

        Map<Field, Object> injectMap = createInjectMap(workerClass);
        Map<Enum, Probe> operationProbeMap = createOperationProbeMap(workerClass, workerInstance);

        // everything is prepared, we can notify the outside world now
        testStartedTimestamp = System.currentTimeMillis();
        isRunning = true;

        // spawn workers and wait for completion
        IWorker worker = runWorkers(runWithWorkerThreadCount, runMethod, injectMap, operationProbeMap);

        // call the afterCompletion() method on a single instance of the worker
        worker.afterCompletion();
    }

    private void invokeTimestepMethod() throws Exception {
        LOGGER.info(format("Spawning %d worker threads for test %s", runWithWorkerThreadCount, testContext.getTestId()));
        if (runWithWorkerThreadCount <= 0) {
            return;
        }

        // create instance to get the class of the IWorker implementation
        workerInstance = invokeMethod(testInstance, runMethod);
        Class<? extends IWorker> workerClass = workerInstance.getClass();

        Map<Field, Object> injectMap = createInjectMap(workerClass);
        Map<Enum, Probe> operationProbeMap = createOperationProbeMap(workerClass, workerInstance);

        // everything is prepared, we can notify the outside world now
        testStartedTimestamp = System.currentTimeMillis();
        isRunning = true;

        // spawn workers and wait for completion
        IWorker worker = runWorkers(runWithWorkerThreadCount, runMethod, injectMap, operationProbeMap);

        // call the afterCompletion() method on a single instance of the worker
        worker.afterCompletion();
    }

    private Map<Field, Object> createInjectMap(Class classType) {
        Map<Field, Object> injectMap = new HashMap<Field, Object>();
        do {
            for (Field field : classType.getDeclaredFields()) {
                Class fieldType = field.getType();
                if (field.isAnnotationPresent(InjectTestContext.class)) {
                    assertFieldType(fieldType, TestContext.class, InjectTestContext.class);
                    injectMap.put(field, testContext);
                } else if (field.isAnnotationPresent(InjectHazelcastInstance.class)) {
                    assertFieldType(fieldType, HazelcastInstance.class, InjectHazelcastInstance.class);
                    injectMap.put(field, testContext.getTargetInstance());
                } else if (field.isAnnotationPresent(InjectProbe.class)) {
                    assertFieldType(fieldType, Probe.class, InjectProbe.class);
                    Probe probe = getOrCreateProbe(getProbeName(field), isPartOfTotalThroughput(field));
                    injectMap.put(field, probe);
                } else if (field.isAnnotationPresent(InjectMetronome.class)) {
                    assertFieldType(fieldType, Metronome.class, InjectMetronome.class);
                    int intervalMillis = getMetronomeIntervalMillis(field, runWithWorkerMetronomeInterval);
                    MetronomeType type = getMetronomeType(field, runWithWorkerMetronomeType);
                    injectMap.put(field, withFixedIntervalMs(intervalMillis, type));
                }
            }
            classType = classType.getSuperclass();
        } while (classType != null);
        return injectMap;
    }

    private Map<Enum, Probe> createOperationProbeMap(Class<? extends IWorker> workerClass, IWorker worker) {
        if (!IMultipleProbesWorker.class.isAssignableFrom(workerClass)) {
            return null;
        }

        // remove the default worker probe
        probeMap.remove(DEFAULT_WORKER_PROBE_NAME);

        Map<Enum, Probe> operationProbes = new HashMap<Enum, Probe>();
        for (Enum operation : ((IMultipleProbesWorker) worker).getOperations()) {
            String probeName = capitalizeFully(operation.name(), '_').replace("_", "") + "Probe";
            operationProbes.put(operation, getOrCreateProbe(probeName, true));
        }
        return operationProbes;
    }

    private Probe getOrCreateProbe(String probeName, boolean partOfTotalThroughput) {
        Probe probe = probeMap.get(probeName);
        if (probe == null) {
            probe = (runWithWorkerIsLightweightProbe
                    ? new ThroughputProbe(partOfTotalThroughput)
                    : new HdrProbe(partOfTotalThroughput));
            probeMap.put(probeName, probe);
        }
        return probe;
    }

    private IWorker runWorkers(int threadCount, Method runMethod, Map<Field, Object> injectMap,
                               Map<Enum, Probe> operationProbes) throws Exception {
        IWorker firstWorker = null;
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            final IWorker worker = invokeMethod(testInstance, runMethod);
            if (firstWorker == null) {
                firstWorker = worker;
            }

            injectObjects(injectMap, worker);
            if (operationProbes != null) {
                ((IMultipleProbesWorker) worker).setProbeMap(operationProbes);
            }
            spawner.spawn(new WorkerTask(worker));
        }
        spawner.awaitCompletion();
        return firstWorker;
    }

    private static Object getTestClassInstance(TestCase testCase) {
        if (testCase == null) {
            throw new NullPointerException();
        }
        String classname = testCase.getClassname();
        Object testObject;
        try {
            ClassLoader classLoader = TestContainer.class.getClassLoader();
            testObject = classLoader.loadClass(classname).newInstance();
        } catch (Exception e) {
            throw new IllegalTestException("Could not create instance of " + classname, e);
        }
        bindProperties(testObject, testCase, OPTIONAL_TEST_PROPERTIES);
        return testObject;
    }

    private static int getThreadCount(TestCase testCase) {
        String propertyValue = getPropertyValue(testCase, THREAD_COUNT_PROPERTY_NAME);
        return (propertyValue == null ? DEFAULT_RUN_WITH_WORKER_THREAD_COUNT : parseInt(propertyValue));
    }

    private static int getMetronomeIntervalProperty(TestCase testCase) {
        String propertyValue = getPropertyValue(testCase, METRONOME_INTERVAL_PROPERTY_NAME);
        return (propertyValue == null ? DEFAULT_RUN_WITH_WORKER_METRONOME_INTERVAL : parseInt(propertyValue));
    }

    private static MetronomeType getMetronomeTypeProperty(TestCase testCase) {
        String propertyValue = getPropertyValue(testCase, METRONOME_TYPE_PROPERTY_NAME);
        return (propertyValue == null ? DEFAULT_RUN_WITH_WORKER_METRONOME_TYPE : MetronomeType.valueOf(propertyValue));
    }

    private static boolean isLightweightProbe(TestCase testCase) {
        String propertyValue = getPropertyValue(testCase, LIGHTWEIGHT_PROBE_PROPERTY_NAME);
        return parseBoolean(propertyValue);
    }

    private static void assertFieldType(Class fieldType, Class expectedFieldType, Class<? extends Annotation> annotation) {
        if (!expectedFieldType.equals(fieldType)) {
            throw new IllegalTestException(format("Found %s annotation on field of type %s, but %s is required!",
                    annotation.getName(), fieldType.getName(), expectedFieldType.getName()));
        }
    }

    private static void injectObjects(Map<Field, Object> injectMap, Object worker) {
        for (Map.Entry<Field, Object> entry : injectMap.entrySet()) {
            setFieldValue(worker, entry.getKey(), entry.getValue());
        }
    }

}
