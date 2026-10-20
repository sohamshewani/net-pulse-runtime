import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class NetPulseRuntimeBenchmark {

    // Simulation of 'net-pulse-runtime' core processing unit
    public static final class NetPulseEngine {
        private final AtomicLong sequence = new AtomicLong(0);

        public void executePulse(byte[] payload) {
            // Simulate lightweight runtime overhead, serialization, and routing check
            long id = sequence.incrementAndGet();
            long hash = 0;
            for (byte b : payload) {
                hash = 31 * hash + b;
            }
            if (id == -1) {
                // Prevent JVM dead-code elimination of the hash calculation
                System.out.println(hash);
            }
        }
    }

    private static final int WARMUP_ITERATIONS = 500_000;
    private static final int MEASUREMENT_ITERATIONS = 2_000_000;
    private static final int CONCURRENCY_THREADS = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) throws InterruptedException {
        NetPulseEngine engine = new NetPulseEngine();
        byte[] samplePayload = "NET-PULSE-RUNTIME-PAYLOAD-V1".getBytes();

        System.out.println("Initializing 'net-pulse-runtime' Microbenchmark...");
        System.out.println("Threads: " + CONCURRENCY_THREADS);
        System.out.println("Warmup Iterations: " + WARMUP_ITERATIONS);
        System.out.println("Measurement Iterations: " + MEASUREMENT_ITERATIONS);
        System.out.println();

        // 1. Warmup Phase (JIT compilation and class loading optimization)
        runPhase(engine, samplePayload, WARMUP_ITERATIONS, false);

        // 2. Measurement Phase
        long[] latencies = runPhase(engine, samplePayload, MEASUREMENT_ITERATIONS, true);

        // 3. Compute Metrics
        Arrays.sort(latencies);
        long totalOps = latencies.length;
        
        long sumNs = 0;
        for (long l : latencies) {
            sumNs += l;
        }
        
        double totalSec = sumNs / 1_000_000_000.0;
        double opsPerSec = totalOps / totalSec;

        long p50 = latencies[(int) (totalOps * 0.50)];
        long p90 = latencies[(int) (totalOps * 0.90)];
        long p99 = latencies[(int) (totalOps * 0.99)];
        long p999 = latencies[(int) (totalOps * 0.999)];
        long max = latencies[latencies.length - 1];

        // 4. Output Formatted Tabular Metrics to stdout
        printTabularResults(totalOps, opsPerSec, p50, p90, p99, p999, max);
    }

    private static long[] runPhase(NetPulseEngine engine, byte[] payload, int totalIterations, boolean recordLatency) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY_THREADS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(CONCURRENCY_THREADS);

        int opsPerThread = totalIterations / CONCURRENCY_THREADS;
        long[][] threadLatencies = new long[CONCURRENCY_THREADS][opsPerThread];

        for (int i = 0; i < CONCURRENCY_THREADS; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    long[] localLatencies = threadLatencies[threadIndex];
                    for (int j = 0; j < opsPerThread; j++) {
                        long start = recordLatency ? System.nanoTime() : 0;
                        engine.executePulse(payload);
                        if (recordLatency) {
                            localLatencies[j] = System.nanoTime() - start;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        if (!recordLatency) {
            return new long[0];
        }

        // Flatten latencies array
        long[] combined = new long[opsPerThread * CONCURRENCY_THREADS];
        int destPos = 0;
        for (long[] arr : threadLatencies) {
            System.arraycopy(arr, 0, combined, destPos, arr.length);
            destPos += arr.length;
        }
        return combined;
    }

    private static void printTabularResults(long totalOps, double opsPerSec, long p50, long p90, long p99, long p999, long max) {
        String lineHeader = "+------------------------+--------------------------+";
        String formatRow = "| %-22s | %-24s |%n";

        System.out.println(lineHeader);
        System.out.printf(formatRow, "Metric", "Result");
        System.out.println(lineHeader);
        System.out.printf(formatRow, "Total Operations", String.format("%,d ops", totalOps));
        System.out.printf(formatRow, "Throughput", String.format("%,.2f ops/sec", opsPerSec));
        System.out.printf(formatRow, "Latency p50", formatNs(p50));
        System.out.printf(formatRow, "Latency p90", formatNs(p90));
        System.out.printf(formatRow, "Latency p99", formatNs(p99));
        System.out.printf(formatRow, "Latency p99.9", formatNs(p999));
        System.out.printf(formatRow, "Latency Max", formatNs(max));
        System.out.println(lineHeader);
    }

    private static String formatNs(long ns) {
        if (ns >= 1_000_000) {
            return String.format("%.3f ms", ns / 1_000_000.0);
        } else if (ns >= 1_000) {
            return String.format("%.3f us", ns / 1_000.0);
        } else {
            return ns + " ns";
        }
    }
}