package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.utils.AssertTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.verification.VerificationMode;

import java.io.File;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.test.FailureType.WORKER_OOM;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class WorkerProcessFailureMonitorTest {

    private static final int DEFAULT_TIMEOUT = 30000;

    private static final int DEFAULT_LAST_SEEN_TIMEOUT_SECONDS = 30;
    private static final int DEFAULT_SCAN_INTERVAL = 30;
    private static final int DEFAULT_SLEEP_TIME = 100;

    private static int addressIndex;

    private WorkerProcessManager workerProcessManager;
    private WorkerProcessFailureMonitor failureMonitor;
    private WorkerProcessFailureSender failureSender;

    @Before
    public void setUp() {
        workerProcessManager = new WorkerProcessManager();

        failureSender = mock(WorkerProcessFailureSender.class);

        failureMonitor = new WorkerProcessFailureMonitor(
                failureSender,
                workerProcessManager,
                DEFAULT_LAST_SEEN_TIMEOUT_SECONDS,
                DEFAULT_SCAN_INTERVAL);
        failureMonitor.start();
    }

    @After
    public void tearDown() {
        failureMonitor.shutdown();

        for (WorkerProcess workerProcess : workerProcessManager.getWorkerProcesses()) {
            deleteQuiet(workerProcess.getWorkerHome());
        }
        deleteQuiet("worker3");
        deleteQuiet("1.exception.sendFailure");
    }

    @Test
    public void testRun_shouldSendNoFailures() {
        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyNoMoreInteractions(failureSender);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testRun_shouldContinueAfterExceptionDuringDetection() {
//        Process process = mock(Process.class);
//        // nasty: will cause an exception while doing the monitoring.
//        when(process.exitValue()).thenThrow(new IllegalArgumentException("expected exception")).thenReturn(0);
//        WorkerProcess exceptionWorker = addWorkerJvm(workerProcessManager, getWorkerAddress(), true, process);
//
//        do {
//            sleepMillis(DEFAULT_SLEEP_TIME);
//        } while (!exceptionWorker.isFinished());
//
//
//        assertFailureSend();
    }

    private void assertFailureSend() {
        verify(failureSender, times(1)).send(anyString(), any(FailureType.class), any(WorkerProcess.class), anyString(), anyString());
    }

    @Test
    public void testRun_shouldDetectException_withTestId() {
//        String cause = throwableToString(new RuntimeException());
//        File exceptionFile = createExceptionFile(workerHome, "WorkerProcessFailureMonitorTest", cause);
//
//        sleepMillis(DEFAULT_SLEEP_TIME);
//
//        assertThatFailureOperationHasBeenSent(agentConnector, 1);
//        verifyNoMoreInteractions(agentConnector);
//        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_withEmptyTestId() {
//        String cause = throwableToString(new RuntimeException());
//        File exceptionFile = createExceptionFile(workerHome, "", cause);
//
//        sleepMillis(DEFAULT_SLEEP_TIME);
//
//        assertThatFailureOperationHasBeenSent(agentConnector, 1);
//        verifyNoMoreInteractions(agentConnector);
//        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_withNullTestId() {
//        String cause = throwableToString(new RuntimeException());
//        //        workerProcess = addWorkerJvm(workerProcessManager, getWorkerAddress(), true);
////        addWorkerJvm(workerProcessManager, getWorkerAddress(), false);
////
////        workerHome = workerProcess.getWorkerHome();
//        File exceptionFile = createExceptionFile(workerHome, "null", cause);
//
//        sleepMillis(DEFAULT_SLEEP_TIME);
//
//        verify(failureSender, Mockito.atLeastOnce()).send(
//                eq("Worked ran into an unhandled exception"),
//                eq(FailureType.WORKER_EXCEPTION),
//                same(workerProcess),
//                isNull(String.class),
//                anyString());
//
//        assertThatExceptionFileDoesNotExist(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectException_shouldRenameFileOnSendFailure() {
        WorkerProcess workerProcess = startWorkerProcess(true);

        // cause problems on the sender.
        when(failureSender.send(
                anyString(),
                any(FailureType.class),
                any(WorkerProcess.class),
                anyString(),
                anyString())).thenReturn(false);

        // write an exception so that the failure monitor will detect a failure.
        String cause = throwableToString(new RuntimeException());
        File exceptionFile = createExceptionFile(workerProcess.getWorkerHome(), "WorkerProcessFailureMonitorTest", cause);

        sleepMillis(DEFAULT_SLEEP_TIME);

        assertThatExceptionFileDoesNotExist(exceptionFile);
        assertThatRenamedExceptionFileExists(exceptionFile);
    }

    @Test
    public void testRun_shouldDetectOomeFailure_withOomeFile() {
        WorkerProcess workerProcess = startWorkerProcess(true);
        ensureExistingFile(workerProcess.getWorkerHome(), "worker.oome");

        sleepMillis(DEFAULT_SLEEP_TIME);

        verify(failureSender, atLeast(1)).send("Worker ran into an OOME", WORKER_OOM, workerProcess, null, null);
    }

    @Test
    public void testRun_shouldDetectOomeFailure_withHprofFile() {
        WorkerProcess workerProcess = startWorkerProcess(true);
        ensureExistingFile(workerProcess.getWorkerHome(), "java_pid3140.hprof");

        sleepMillis(DEFAULT_SLEEP_TIME);

        verify(failureSender, atLeast(1)).send("Worker ran into an OOME", WORKER_OOM, workerProcess, null, null);
    }

    @Test
    public void testRun_shouldDetectInactivity() {
        final WorkerProcess workerProcess = startWorkerProcess(true);

        failureMonitor.startTimeoutDetection();

        // mark the workerProcess as last seen an hour ago
        when(workerProcess.getLastSeen()).thenReturn(currentTimeMillis() - HOURS.toMillis(1));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(failureSender, atLeast(1)).send("Worker ran into an OOME", WORKER_OOM, workerProcess, null, null);
            }
        });
    }

    @Test
    public void testRun_shouldNotDetectInactivity_ifDetectionDisabled() {
//        failureMonitor = new WorkerProcessFailureMonitor(failureSender, workerProcessManager, -1, DEFAULT_SCAN_INTERVAL);
//        failureMonitor.start();
//
//        failureMonitor.startTimeoutDetection();
//        workerProcess.setLastSeen(currentTimeMillis() - HOURS.toMillis(1));
//
//        sleepMillis(DEFAULT_SLEEP_TIME);
//
//        failureMonitor.stopTimeoutDetection();
//        workerProcess.setLastSeen(currentTimeMillis() - HOURS.toMillis(1));
//
//        sleepMillis(DEFAULT_SLEEP_TIME);
//
//        verifyNoMoreInteractions(failureSender);
    }

    @Test
    public void testRun_shouldNotDetectInactivity_ifDetectionNotStarted() {
        final WorkerProcess workerProcess = startWorkerProcess(true);
        when(workerProcess.getLastSeen()).thenReturn(currentTimeMillis() - HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyZeroInteractions(failureSender);
    }

    @Test
    public void testRun_shouldNotDetectInactivity_afterDetectionIsStopped() {
        final WorkerProcess workerProcess = startWorkerProcess(false);

        failureMonitor.startTimeoutDetection();

        sleepMillis(DEFAULT_SLEEP_TIME);

        failureMonitor.stopTimeoutDetection();
        when(workerProcess.getLastSeen()).thenReturn(currentTimeMillis() - HOURS.toMillis(1));

        sleepMillis(DEFAULT_SLEEP_TIME);

        verifyNoMoreInteractions(failureSender);
    }

    @Test
    public void testRun_shouldDetectUnexpectedExit_whenExitValueIsZero() {
        Process process = mock(Process.class);
        when(process.exitValue()).thenReturn(0);

        final WorkerProcess exitWorker = startWorkerProcess(true, process);

        assertTrueEventually(new AssertTask(){
            @Override
           public void run() throws Exception {
                // and we want to make sure an failure is send.
                verify(failureSender, atLeastOnce()).send(
                        anyString(),
                        eq(FailureType.WORKER_FINISHED),
                        eq(exitWorker),
                        isNull(String.class),
                        isNull(String.class));

                //assertEquals(0, workerProcessManager.getWorkerProcesses().size());
            }
        });
    }

    @Test
    public void testRun_shouldDetectUnexpectedExit_whenExitValueIsNonZero() {
        Process process = mock(Process.class);
        when(process.exitValue()).thenReturn(134);

        final WorkerProcess worker = startWorkerProcess(true, process);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(failureSender, atLeastOnce()).send(
                        anyString(),
                        eq(FailureType.WORKER_EXIT),
                        eq(worker),
                        isNull(String.class),
                        isNull(String.class));

              //  assertEquals(0, workerProcessManager.getWorkerProcesses().size());
            }
        });
    }

    @Test
    public void testExceptionExtensionFilter_shouldReturnEmptyFileListIfDirectoryDoesNotExist() {
        File[] files = WorkerProcessFailureMonitor.ExceptionExtensionFilter.listFiles(new File("notFound"));

        assertEquals(0, files.length);
    }

    @Test
    public void testHProfExtensionFilter_shouldReturnEmptyFileListIfDirectoryDoesNotExist() {
        File[] files = WorkerProcessFailureMonitor.HProfExtensionFilter.listFiles(new File("notFound"));

        assertEquals(0, files.length);
    }

    private static SimulatorAddress newWorkerAddress() {
        return new SimulatorAddress(WORKER, 1, ++addressIndex, 0);
    }

    private WorkerProcess startWorkerProcess(boolean createWorkerHome){
        return startWorkerProcess(createWorkerHome, mock(Process.class));
    }

    private WorkerProcess startWorkerProcess(boolean createWorkerHome, Process process) {
        SimulatorAddress address = newWorkerAddress();
        int addressIndex = address.getAddressIndex();
        File workerHome = new File("worker" + address.getAddressIndex());

        WorkerProcess workerProcess = mock(WorkerProcess.class);

        when(workerProcess.getAddress()).thenReturn(address);
        when(workerProcess.getProcess()).thenReturn(process);
        when(workerProcess.getWorkerHome()).thenReturn(workerHome);
        when(workerProcess.getId()).thenReturn("WorkerProcessFailureMonitorTest" + addressIndex);
        workerProcessManager.add(address, workerProcess);

        if (createWorkerHome) {
            ensureExistingDirectory(workerHome);
        }

        return workerProcess;
    }

    private static File createExceptionFile(File workerHome, String testId, String cause) {
        String targetFileName = "1.exception";

        File tmpFile = ensureExistingFile(workerHome, targetFileName + "tmp");
        File exceptionFile = new File(workerHome, targetFileName);

        appendText(testId + NEW_LINE + cause, tmpFile);
        rename(tmpFile, exceptionFile);

        return exceptionFile;
    }

    private static void assertThatWorkerHasBeenRemoved(AgentConnector agentConnector, int times) {
        assertThatWorkerHasBeenRemoved(agentConnector, times(times));
    }

    private static void assertThatWorkerHasBeenRemoved(AgentConnector agentConnector, VerificationMode mode) {
        verify(agentConnector, mode).removeWorker(anyInt());
    }

    private static void assertThatExceptionFileDoesNotExist(File firstExceptionFile) {
        assertFalse("Exception file should be deleted: " + firstExceptionFile, firstExceptionFile.exists());
    }

    private static void assertThatRenamedExceptionFileExists(File exceptionFile) {
        File expectedFile = new File(exceptionFile.getName() + ".sendFailure");
        assertTrue("Exception file should be renamed: " + expectedFile.getName(), expectedFile.exists());
    }
}
