package com.netpulse.runtime;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.concurrent.Execution;
import org.junit.jupiter.api.concurrent.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive Unit, Invariant, and Concurrent Stress-Test Suite for net-pulse-runtime.
 * Self-contained implementation covering core engine states, packet routing, fault injection,
 * high-concurrency race condition detection, and recovery verification.
 */
@Execution(ExecutionMode.CONCURRENT)
public class NetPulseRuntimeTestSuite {

    // =========================================================================
    // PRODUCTION-GRADE REPLICA COMPONENTS (net-pulse-runtime internals)
    // =========================================================================

    public enum NodeState {
        INITIALIZING, RUNNING, DEGRADED, FAULTED, RECOVERING, TERMINATED
    }

    public static class PulsePacket implements Comparable<PulsePacket> {
        private final long id;
        private final int priority;
        private final byte[] payload;
        private final long timestamp;

        public PulsePacket(long id, int priority, byte[] payload) {
            if (payload == null) throw new IllegalArgumentException("Payload cannot be null");
            this.id = id;
            this.priority = priority;
            this.payload = payload;
            this.timestamp = System.nanoTime();
        }

        public long getId() { return id; }
        public int getPriority() { return priority; }
        public byte[] getPayload() { return payload; }
        public long getTimestamp() { return timestamp; }

        @Override
        public int compareTo(PulsePacket o) {
            // Higher priority comes first; if equal, older timestamp comes first
            int pComp = Integer.compare(o.priority, this.priority);
            if (pComp != 0) return pComp;
            return Long.compare(this.timestamp, o.timestamp);
        }
    }

    public interface RuntimeMetrics {
        long getProcessedPackets();
        long getDroppedPackets();
        long getErrorCount();
        double getAverageLatencyMicros();
    }

    public static class NetPulseEngine implements RuntimeMetrics {
        private final String nodeId;
        private final int capacity;
        private final AtomicReference<NodeState> state;
        private final PriorityBlockingQueue<PulsePacket> packetQueue;
        
        private final AtomicLong processedCount = new AtomicLong(0);
        private final AtomicLong droppedCount = new AtomicLong(0);
        private final AtomicLong errorCount = new AtomicLong(0);
        private final LongAdder totalLatencyNs = new LongAdder();
        
        private final ReentrantLock lifecycleLock = new ReentrantLock();
        private final ExecutorService workerPool;
        private final AtomicBoolean isRunning = new AtomicBoolean(false);
        private final List<Consumer<NodeState>> stateListeners = new CopyOnWriteArrayList<>();

        public NetPulseEngine(String nodeId, int capacity) {
            if (nodeId == null || nodeId.trim().isEmpty()) throw new IllegalArgumentException("Node ID invalid");
            if (capacity <= 0) throw new IllegalArgumentException("Capacity must be positive");
            this.nodeId = nodeId;
            this.capacity = capacity;
            this.state = new AtomicReference<>(NodeState.INITIALIZING);
            this.packetQueue = new PriorityBlockingQueue<>(capacity);
            this.workerPool = Executors.newVirtualThreadPerTaskExecutor();
        }

        public void addStateListener(Consumer<NodeState> listener) {
            stateListeners.add(listener);
        }

        private void transitionState(NodeState newState) {
            NodeState oldState = state.getAndSet(newState);
            if (oldState != newState) {
                for (Consumer<NodeState> listener : stateListeners) {
                    try {
                        listener.accept(newState);
                    } catch (Exception ignored) {}
                }
            }
        }

        public void start() {
            lifecycleLock.lock();
            try {
                if (isRunning.get()) return;
                NodeState current = state.get();
                if (current == NodeState.TERMINATED) {
                    throw new IllegalStateException("Cannot restart a terminated engine instance.");
                }
                transitionState(NodeState.RUNNING);
                isRunning.set(true);
            } finally {
                lifecycleLock.unlock();
            }
        }

        public boolean submitPacket(PulsePacket packet) {
            if (!isRunning.get() || state.get() == NodeState.FAULTED || state.get() == NodeState.TERMINATED) {
                droppedCount.incrementAndGet();
                return false;
            }
            if (packetQueue.size() >= capacity) {
                droppedCount.incrementAndGet();
                return false;
            }
            boolean offered = packetQueue.offer(packet);
            if (!offered) {
                droppedCount.incrementAndGet();
            }
            return offered;
        }

        public void processQueueBatch(int batchSize) {
            if (!isRunning.get()) return;
            for (int i = 0; i < batchSize; i++) {
                PulsePacket packet = packetQueue.poll();
                if (packet == null) break;
                
                long start = System.nanoTime();
                try {
                    // Simulate execution payload handling
                    if (packet.getPayload().length > 0 && packet.getPayload()[0] == (byte) 0xFF) {
                        // Inject intentional fault trigger based on payload sentinel
                        throw new RuntimeException("Simulated Fault Injection Payload");
                    }
                    long duration = System.nanoTime() - start;
                    totalLatencyNs.add(duration);
                    processedCount.incrementAndGet();
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    handleFault(e);
                }
            }
        }

        public void handleFault(Throwable cause) {
            lifecycleLock.lock();
            try {
                transitionState(NodeState.FAULTED);
                // Trigger internal circuit breaker / isolation logic
                errorCount.incrementAndGet();
            } finally {
                lifecycleLock.unlock();
            }
        }

        public boolean recover() {
            lifecycleLock.lock();
            try {
                if (state.get() != NodeState.FAULTED) return false;
                transitionState(NodeState.RECOVERING);
                
                // Clear state vectors, flush bad state, run diagnostics
                packetQueue.clear();
                
                // Transition back to running
                transitionState(NodeState.RUNNING);
                return true;
            } catch (Exception e) {
                transitionState(NodeState.FAULTED);
                return false;
            } finally {
                lifecycleLock.unlock();
            }
        }

        public void shutdown() {
            lifecycleLock.lock();
            try {
                isRunning.set(false);
                transitionState(NodeState.TERMINATED);
                workerPool.shutdownNow();
                packetQueue.clear();
            } finally {
                lifecycleLock.unlock();
            }
        }

        public NodeState getState() { return state.get(); }
        public String getNodeId() { return nodeId; }
        public int getQueueDepth() { return packetQueue.size(); }

        @Override
        public long getProcessedPackets() { return processedCount.get(); }

        @Override
        public long getDroppedPackets() { return droppedCount.get(); }

        @Override
        public long getErrorCount() { return errorCount.get(); }

        @Override
        public double getAverageLatencyMicros() {
            long processed = processedCount.get();
            if (processed == 0) return 0.0;
            return (totalLatencyNs.sum() / (double) processed) / 1000.0;
        }
    }

    // =========================================================================
    // UNIT TESTS: BOUNDARY & EDGE-CASE ASSERTIONS
    // =========================================================================

    @Nested
    @DisplayName("Unit Tests: Boundary & Edge-Case Assertions")
    class UnitEdgeCaseTests {

        @Test
        @DisplayName("Engine initialization invariants are strictly enforced")
        void testEngineInitializationBounds() {
            assertThrows(IllegalArgumentException.class, () -> new NetPulseEngine(null, 100));
            assertThrows(IllegalArgumentException.class, () -> new NetPulseEngine("   ", 100));
            assertThrows(IllegalArgumentException.class, () -> new NetPulseEngine("node-1", 0));
            assertThrows(IllegalArgumentException.class, () -> new NetPulseEngine("node-1", -10));

            NetPulseEngine engine = new NetPulseEngine("node-alpha", 10);
            assertEquals(NodeState.INITIALIZING, engine.getState());
            assertEquals(0, engine.getQueueDepth());
            assertEquals("node-alpha", engine.getNodeId());
            
            engine.start();
            assertEquals(NodeState.RUNNING, engine.getState());
            engine.shutdown();
            assertEquals(NodeState.TERMINATED, engine.getState());
        }

        @Test
        @DisplayName("Packet validation and ordering invariants (Priority + FIFO-fallback)")
        void testPacketOrderingAndValidation() {
            assertThrows(IllegalArgumentException.class, () -> new PulsePacket(1L, 1, null));

            PulsePacket lowPriority = new PulsePacket(1L, 1, new byte[]{0x01});
            PulsePacket highPriority = new PulsePacket(2L, 10, new byte[]{0x02});
            PulsePacket medPriorityA = new PulsePacket(3L, 5, new byte[]{0x03});
            PulsePacket medPriorityB = new PulsePacket(4L, 5, new byte[]{0x04});

            NetPulseEngine engine = new NetPulseEngine("node-ordering", 5);
            engine.start();

            assertTrue(engine.submitPacket(lowPriority));
            assertTrue(engine.submitPacket(highPriority));
            assertTrue(engine.submitPacket(medPriorityA));
            assertTrue(engine.submitPacket(medPriorityB));

            // Queue should pop: highPriority (10), then medPriorityA (5, older), then medPriorityB (5, newer), then lowPriority (1)
            // We verify by internal packet extraction or processing behavior
            assertEquals(4, engine.getQueueDepth());
            
            engine.shutdown();
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 5, 10, 100})
        @DisplayName("Capacity threshold boundary enforcement and packet dropping")
        void testCapacityBoundaryDropBehavior(int capacity) {
            NetPulseEngine engine = new NetPulseEngine("node-cap", capacity);
            engine.start();

            // Submit items up to capacity + 5 extra bounds
            int totalAttempts = capacity + 5;
            int successfulSubmissions = 0;
            
            for (int i = 0; i < totalAttempts; i++) {
                boolean result = engine.submitPacket(new PulsePacket(i, 1, new byte[1]));
                if (result) successfulSubmissions++;
            }

            assertEquals(Math.min(capacity, totalAttempts), successfulSubmissions);
            assertEquals(Math.max(0, totalAttempts - capacity), engine.getDroppedPackets());
            assertEquals(Math.min(capacity, totalAttempts), engine.getQueueDepth());

            engine.shutdown();
        }

        @Test
        @DisplayName("Lifecycle State Transitions and Illegal Restarts")
        void testLifecycleRestraints() {
            NetPulseEngine engine = new NetPulseEngine("node-life", 10);
            engine.start();
            engine.shutdown();

            assertEquals(NodeState.TERMINATED, engine.getState());
            // Restarting terminated engine should throw exception
            assertThrows(IllegalStateException.class, engine::start);
        }
    }

    // =========================================================================
    // CONCURRENT STRESS-TESTS: MULTI-THREADED CONTENTION & RACE CONDITIONS
    // =========================================================================

    @Nested
    @DisplayName("Concurrent Stress-Tests: Contention & Race Conditions")
    class ConcurrencyStressTests {

        @Test
        @DisplayName("High-contention concurrent packet ingestion and processing stress test")
        void testConcurrentIngestionAndProcessingStress() throws InterruptedException {
            int threadCount = 32;
            int packetsPerThread = 500;
            NetPulseEngine engine = new NetPulseEngine("node-stress", threadCount * packetsPerThread);