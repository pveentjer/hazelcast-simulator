import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MemoryBandwidth {
    private static final Unsafe unsafe = loadUnsafe();

    public static final long GIGABYTE = 1024l * 1024 * 1024;

    private int iterations;
    private long sizeGB;
    private long sizeB;
    private int threadCount;
    private ExecutorService executor;
    private long address;
    private long totalDurationMs;

    public static void main(String[] args) throws Exception {
        MemoryBandwidth main = new MemoryBandwidth();
        main.init(args);
        main.warmup();
        main.run();
        main.report();
        System.exit(0);
    }

    private void init(String[] args) {
        iterations = Integer.parseInt(args[0]);
        sizeGB = Integer.parseInt(args[1]);
        threadCount = Integer.parseInt(args[2]);

        System.out.println("Iterations: " + iterations);
        System.out.println("Memory size: " + sizeGB + " GB");
        System.out.println("Thread count: " + threadCount);

        executor = Executors.newFixedThreadPool(threadCount);
        sizeB = sizeGB * GIGABYTE;

        System.out.println("Allocating memory");
        address = unsafe.allocateMemory(sizeB);
    }

    private void warmup() throws Exception {
        System.out.println("Warmup starting");

        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<Long>> futures = new ArrayList<Future<Long>>(threadCount);
        for (int k = 0; k < threadCount; k++) {
            futures.add(executor.submit(new LoadTask(startLatch, k)));
        }

        startLatch.countDown();

        long durationMs = 0;
        for (Future<Long> future : futures) {
            durationMs += future.get();
        }

        System.out.println("Warmup completed in:" + durationMs + " ms");
    }

    private void run() throws Exception {
        for (int l = 0; l < iterations; l++) {
            System.out.println("#" + (l + 1) + " starting");

            CountDownLatch startLatch = new CountDownLatch(1);
            List<Future<Long>> futures = new ArrayList<Future<Long>>(threadCount);
            for (int k = 0; k < threadCount; k++) {
                futures.add(executor.submit(new LoadTask(startLatch, k)));
            }

            startLatch.countDown();

            long durationMs = 0;
            for (Future<Long> future : futures) {
                durationMs += future.get();
            }

            System.out.println("#" + (l + 1) + " completed in:" + durationMs + " ms");
            totalDurationMs += durationMs;
        }
    }

    private void report() {
        long gigabytesTraversed = iterations * sizeGB;
        System.out.println("Duration:" + totalDurationMs + " ms");
        System.out.println("Total traversed:" + gigabytesTraversed + " GB");
        double gigabytesTraversedPerMs = (1.0d * gigabytesTraversed) / totalDurationMs;
        System.out.println("Bandwidth:" + (gigabytesTraversedPerMs * SECONDS.toMillis(1)) + " GB/s");
    }

    private static Unsafe loadUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Unsafe", e);
        }
    }

    private class LoadTask implements Callable<Long> {

        private final CountDownLatch startLatch;
        private final long startAddress;
        private final long endAddress;

        LoadTask(CountDownLatch startLatch, int index) {
            this.startLatch = startLatch;
            long sizePerThread = sizeB / threadCount;
            this.startAddress = address + (index * sizePerThread);
            this.endAddress = startAddress + sizePerThread;
        }

        @Override
        public Long call() throws Exception {
            startLatch.await();
            long buggers = 0;
            long startMs = System.currentTimeMillis();
            for (long ptr = startAddress; ptr < endAddress; ptr += 20) {
                buggers += unsafe.getByte(ptr);
            }
            long durationMs = System.currentTimeMillis() - startMs;
            System.out.println("        " + Thread.currentThread().getName() + " completed in " + durationMs + " ms, with buggers:" + buggers);
            return durationMs;
        }
    }
}
