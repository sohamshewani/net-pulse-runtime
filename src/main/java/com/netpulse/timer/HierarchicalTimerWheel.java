package com.netpulse.timer;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class HierarchicalTimerWheel {

    private static final Logger LOGGER = Logger.getLogger(HierarchicalTimerWheel.class.getName());
    private static final Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new AssertionError("Unable to initialize Unsafe", e);
        }
    }

    private static final int WHEEL_COUNT = 4;
    private static final int RESOLUTION_BITS = 8; // 256 slots per wheel
    private static final int WHEEL_SIZE = 1 << RESOLUTION_BITS; // 256
    private static final int WHEEL_MASK = WHEEL_SIZE - 1;
    
    private final long tickDurationNanos;
    private final long startTimeNanos;
    
    private long ticks;
    
    // Memory layout: 4 wheels, each wheel has 256 buckets.
    // Each bucket is the head of a doubly-linked list of TimerTask nodes.
    // To maintain zero-allocation hot paths and strict cache alignment, we store node pointers 
    // off-heap or via direct array structures. Here we allocate direct native memory for the bucket heads.
    // Each bucket head contains: [head_address (8 bytes), tail_address (8 bytes)]. Size = 16 bytes per bucket.
    // Total buckets = 4 * 256 = 1024 buckets. Total memory = 1024 * 16 = 16384 bytes.
    private final long wheelsMemoryAddress;

    // TimerTask layout off-heap offsets:
    // offset 0: next pointer (8 bytes)
    // offset 8: prev pointer (8 bytes)
    // offset 16: expiration time in nanos (8 bytes)
    // offset 24: task id / correlation (8 bytes)
    // offset 32: wheel index (4 bytes)
    // offset 36: bucket index (4 bytes)
    // offset 40: cancelled flag (1 byte, padded to 64 bytes cache line alignment)
    private static final long TASK_STRUCT_SIZE = 64;

    public HierarchicalTimerWheel(long tickDurationNanos) {
        if (tickDurationNanos <= 0) {
            throw new IllegalArgumentException("tickDurationNanos must be positive");
        }
        this.tickDurationNanos = tickDurationNanos;
        this.startTimeNanos = System.nanoTime();
        this.ticks = 0;

        long totalBytes = (long) WHEEL_COUNT * WHEEL_SIZE * 16; // 16 bytes per bucket (head + tail)
        this.wheelsMemoryAddress = UNSAFE.allocateMemory(totalBytes);
        UNSAFE.setMemory(this.wheelsMemoryAddress, totalBytes, (byte) 0);
    }

    public long schedule(Runnable task, long delayNanos) {
        Objects.requireNonNull(task, "task cannot be null");
        if (delayNanos < 0) {
            delayNanos = 0;
        }

        long expirationNanos = System.nanoTime() + delayNanos;
        long taskAddress = UNSAFE.allocateMemory(TASK_STRUCT_SIZE);

        // Initialize task structure
        UNSAFE.putLong(taskAddress, 0L);       // next = null
        UNSAFE.putLong(taskAddress + 8, 0L);   // prev = null
        UNSAFE.putLong(taskAddress + 16, expirationNanos);
        
        // Wrap Runnable into task data or use a reference registry. 
        // For zero-allocation design, we store the identityHashCode or map it. 
        // Here we store the object reference in a concurrent handle map or execute directly.
        // To strictly adhere to zero-object-allocation hot path and maintain raw source validity:
        long taskId = TaskRegistry.register(task);
        UNSAFE.putLong(taskAddress + 24, taskId);

        insert(taskAddress);
        return taskId;
    }

    public void cancel(long taskId) {
        long taskAddress = TaskRegistry.findAndRemoveAddress(taskId);
        if (taskAddress != 0L) {
            removeTask(taskAddress);
            UNSAFE.freeMemory(taskAddress);
        }
    }

    public void tick() {
        long currentTime = System.nanoTime();
        long elapsedNanos = currentTime - startTimeNanos;
        long targetTicks = elapsedNanos / tickDurationNanos;

        while (ticks < targetTicks) {
            cascadeAndExecute(ticks);
            ticks++;
        }
    }

    private void insert(long taskAddress) {
        long expirationNanos = UNSAFE.getLong(taskAddress + 16);
        long elapsedNanos = Math.max(0L, expirationNanos - startTimeNanos);
        long taskTicks = elapsedNanos / tickDurationNanos;
        long ticksDiff = taskTicks - ticks;

        int wheelIndex;
        int bucketIndex;

        if (ticksDiff < 0) {
            ticksDiff = 0;
        }

        // Determine which wheel tier this task belongs to based on magnitude of ticksDiff
        long interval = ticksDiff;
        if (interval < WHEEL_SIZE) {
            wheelIndex = 0;
            bucketIndex = (int)((ticks + interval) & WHEEL_MASK);
        } else if (interval < (1L << (2 * RESOLUTION_BITS))) {
            wheelIndex = 1;
            bucketIndex = (int)((ticks + interval) >> RESOLUTION_BITS) & WHEEL_MASK;
        } else if (interval < (1L << (3 * RESOLUTION_BITS))) {
            wheelIndex = 2;
            bucketIndex = (int)((ticks + interval) >> (2 * RESOLUTION_BITS)) & WHEEL_MASK;
        } else {
            wheelIndex = 3;
            bucketIndex = (int)((ticks + interval) >> (3 * RESOLUTION_BITS)) & WHEEL_MASK;
        }

        UNSAFE.putInt(taskAddress + 32, wheelIndex);
        UNSAFE.putInt(taskAddress + 36, bucketIndex);

        addToBucket(wheelIndex, bucketIndex, taskAddress);
    }

    private void addToBucket(int wheelIndex, int bucketIndex, long taskAddress) {
        long bucketAddress = getBucketAddress(wheelIndex, bucketIndex);
        long head = UNSAFE.getLong(bucketAddress);
        long tail = UNSAFE.getLong(bucketAddress + 8);

        if (head == 0L) {
            UNSAFE.putLong(bucketAddress, taskAddress);
            UNSAFE.putLong(bucketAddress + 8, taskAddress);
            UNSAFE.putLong(taskAddress, 0L);
            UNSAFE.putLong(taskAddress + 8, 0L);
        } else {
            UNSAFE.putLong(tail, taskAddress);
            UNSAFE.putLong(taskAddress + 8, tail);
            UNSAFE.putLong(taskAddress, 0L);
            UNSAFE.putLong(bucketAddress + 8, taskAddress);
        }
    }

    private void removeTask(long taskAddress) {
        int wheelIndex = UNSAFE.getInt(taskAddress + 32);
        int bucketIndex = UNSAFE.getInt(taskAddress + 36);

        long next = UNSAFE.getLong(taskAddress);
        long prev = UNSAFE.getLong(taskAddress + 8);

        if (prev != 0L) {
            UNSAFE.putLong(prev, next);
        } else {
            // It was the head
            long bucketAddress = getBucketAddress(wheelIndex, bucketIndex);
            UNSAFE.putLong(bucketAddress, next);
        }

        if (next != 0L) {
            UNSAFE.putLong(next + 8, prev);
        } else {
            // It was the tail
            long bucketAddress = getBucketAddress(wheelIndex, bucketIndex);
            UNSAFE.putLong(bucketAddress + 8, prev);
        }

        UNSAFE.putLong(taskAddress, 0L);
        UNSAFE.putLong(taskAddress + 8, 0L);
    }

    private long getBucketAddress(int wheelIndex, int bucketIndex) {
        int bucketOffset = (wheelIndex * WHEEL_SIZE + bucketIndex) * 16;
        return wheelsMemoryAddress + bucketOffset;
    }

    private void cascadeAndExecute(long currentTick) {
        int wheel0Bucket = (int)(currentTick & WHEEL_MASK);
        
        // Execute tasks in Wheel 0, bucket corresponding to currentTick
        processBucket(0, wheel0Bucket);

        // Check for rollovers into higher wheels
        if (wheel0Bucket == 0) {
            long wheel1Tick = (currentTick >> RESOLUTION_BITS);
            int wheel1Bucket = (int)(wheel1Tick & WHEEL_MASK);
            cascadeWheel(1, wheel1Bucket);

            if (wheel1Bucket == 0) {
                long wheel2Tick = (currentTick >> (2 * RESOLUTION_BITS));
                int wheel2Bucket = (int)(wheel2Tick & WHEEL_MASK);
                cascadeWheel(2, wheel2Bucket);

                if (wheel2Bucket == 0) {
                    long wheel3Tick = (currentTick >> (3 * RESOLUTION_BITS));
                    int wheel3Bucket = (int)(wheel3Tick & WHEEL_MASK);
                    cascadeWheel(3, wheel3Bucket);
                }
            }
        }
    }

    private void processBucket(int wheelIndex, int bucketIndex) {
        long bucketAddress = getBucketAddress(wheelIndex, bucketIndex);
        long currentTask = UNSAFE.getLong(bucketAddress);

        // Clear bucket head/tail
        UNSAFE.putLong(bucketAddress, 0L);
        UNSAFE.putLong(bucketAddress + 8, 0L);

        while (currentTask != 0L) {
            long nextTask = UNSAFE.getLong(currentTask);
            // Unlink pointers on the task safely
            UNSAFE.putLong(currentTask, 0L);
            UNSAFE.putLong(currentTask + 8, 0L);

            long expirationNanos = UNSAFE.getLong(currentTask + 16);
            if (System.nanoTime() >= expirationNanos) {
                executeTask(currentTask);
            } else {
                // Re-insert if prematurely triggered due to cascading granularity
                insert(currentTask);
            }

            currentTask = nextTask;
        }
    }

    private void cascadeWheel(int wheelIndex, int bucketIndex) {
        long bucketAddress = getBucketAddress(wheelIndex, bucketIndex);
        long currentTask = UNSAFE.getLong(bucketAddress);

        UNSAFE.putLong(bucketAddress, 0L);
        UNSAFE.putLong(bucketAddress + 8, 0L);

        while (currentTask != 0L) {
            long nextTask = UNSAFE.getLong(currentTask);
            UNSAFE.putLong(currentTask, 0L);
            UNSAFE.putLong(currentTask + 8, 0L);

            // Re-insert task into lower wheels or appropriate tier
            insert(currentTask);

            currentTask = nextTask;
        }
    }

    private void executeTask(long taskAddress) {
        long taskId = UNSAFE.getLong(taskAddress + 24);
        Runnable task = TaskRegistry.findAndRemoveAddress(taskId) != 0L ? TaskRegistry.getTask(taskId) : null;
        
        // Free native memory footprint immediately to prevent leaks
        UNSAFE.freeMemory(taskAddress);

        if (task != null) {
            try {
                task.run();
            } catch (Throwable t) {
                // Failure Recovery Mode: Cascading Timer Exception Isolation
                LOGGER.log(Level.SEVERE, "Uncaught exception in scheduled timer task execution", t);
            }
        }
    }

    public void close() {
        // Drain all buckets and release native memory allocations
        for (int w = 0; w < WHEEL_COUNT; w++) {
            for (int b = 0; b < WHEEL_SIZE; b++) {
                long bucketAddress = getBucketAddress(w, b);
                long currentTask = UNSAFE.getLong(bucketAddress);
                while (currentTask != 0L) {
                    long nextTask = UNSAFE.getLong(currentTask);
                    long taskId = UNSAFE.getLong(currentTask + 24);
                    TaskRegistry.findAndRemoveAddress(taskId);
                    UNSAFE.freeMemory(currentTask);
                    currentTask = nextTask;
                }
            }
        }
        UNSAFE.freeMemory(wheelsMemoryAddress);
        TaskRegistry.clear();
    }

    private static final class TaskRegistry {
        private static final int CAPACITY = 65536;
        private static final long[] IDs = new long[CAPACITY];
        private static final long[] ADDRESSES = new long[CAPACITY];
        private static final Runnable[] TASKS = new Runnable[CAPACITY];
        private static long sequence = 1L;

        private static synchronized long register(Runnable task) {
            long id = sequence++;
            int index = (int)(id & (CAPACITY - 1));
            IDs[index] = id;
            ADDRESSES[index] = 1L; // placeholder flag indicating active
            TASKS[index] = task;
            return id;
        }

        private static synchronized long findAndRemoveAddress(long id) {
            int index = (int)(id & (CAPACITY - 1));
            if (IDs[index] == id) {
                IDs[index] = 0L;
                long addr = ADDRESSES[index];
                ADDRESSES[index] = 0L;
                return addr;
            }
            return 0L;
        }

        private static synchronized Runnable getTask(long id) {
            int index = (int)(id & (CAPACITY - 1));
            Runnable t = TASKS[index];
            TASKS[index] = null;
            return t;
        }

        private static synchronized void clear() {
            java.util.Arrays.fill(IDs, 0L);
            java.util.Arrays.fill(ADDRESSES, 0L);
            java.util.Arrays.fill(TASKS, null);
        }
    }
}